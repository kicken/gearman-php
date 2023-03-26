<?php
/**
 * Copyright (c) 2015 Keith
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN Server WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

namespace Kicken\Gearman;

use Kicken\Gearman\Client\BackgroundJob;
use Kicken\Gearman\Client\ClientJobData;
use Kicken\Gearman\Client\ForegroundJob;
use Kicken\Gearman\Client\JobStatus;
use Kicken\Gearman\Client\PacketHandler\CreateJobHandler;
use Kicken\Gearman\Client\PacketHandler\JobStatusHandler;
use Kicken\Gearman\Client\PacketHandler\PingHandler;
use Kicken\Gearman\Exception\EmptyServerListException;
use Kicken\Gearman\Exception\NoRegisteredFunctionException;
use Kicken\Gearman\Exception\NoWorkException;
use Kicken\Gearman\Job\Data\JobStatusData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Worker\FunctionRegistry;
use Kicken\Gearman\Worker\PacketHandler\GrabJobHandler;
use Kicken\Gearman\Worker\SleepHandler;
use Kicken\Gearman\Worker\WorkerFunction;
use Kicken\Gearman\Worker\WorkerJob;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Promise\PromiseInterface;
use function React\Promise\all;
use function React\Promise\any;
use function React\Promise\reject;

/**
 * A class for submitting jobs to a Gearman server.
 *
 * @package Kicken\Gearman
 */
class Client {
    use LoggerAwareTrait {
        LoggerAwareTrait::setLogger as originalSetLogger;
    }

    /** @var Endpoint[] */
    private array $serverList;
    private FunctionRegistry $functionList;
    private bool $autoDisconnect = true;
    private bool $stopWorking = false;

    private LoopInterface $loop;

    /**
     * Create a new Gearman Client, used for submitting new jobs or checking the status of existing jobs.
     *
     * @param string|string[]|Endpoint|Endpoint[] $serverList The server(s) to connect to.
     */
    public function __construct($serverList = '127.0.0.1:4730', LoopInterface $loop = null){
        $this->loop = $loop ?? Loop::get();
        $this->serverList = mapToEndpointObjects($serverList, $this->loop);
        $this->functionList = new FunctionRegistry();
        $this->logger = new NullLogger();
    }

    public function setLogger(LoggerInterface $logger){
        $this->originalSetLogger($logger);
        foreach ($this->serverList as $server){
            $server->setLogger($this->logger);
        }
    }

    public function setAutoDisconnect(bool $autoDisconnect) : void{
        $this->autoDisconnect = $autoDisconnect;
    }

    public function pingServer() : PromiseInterface{
        return $this->connect()->then(function(Endpoint $connection){
            return (new PingHandler($this->logger))->ping($connection);
        });
    }

    /**
     * Submit a new job to the server.  Results can be retrieved once the job is complete through the returned
     * ClientJob object.  Use the wait method to wait for the job to complete.
     *
     * @param string $function The function to be run.
     * @param string $workload Data for the function to operate on.
     * @param int $priority One of the JobPriority constants.
     * @param string $unique A unique ID for the job.
     *
     * @return PromiseInterface
     */
    public function submitJob(string $function, string $workload, int $priority = JobPriority::NORMAL, string $unique = '') : PromiseInterface{
        $jobDetails = new ClientJobData($function, $workload, $unique, $priority);

        return $this->connect()->then(function(Endpoint $server) use ($jobDetails){
            return $this->createJob($server, $jobDetails);
        })->then(function() use ($jobDetails){
            return new ForegroundJob($jobDetails);
        });
    }

    /**
     * Submit a new job to the server for execution as a background task.  Background tasks are unable to pass back any
     * result data, but can provide status information regarding the progress of the job.  Status information must be
     * obtained by calling the getJobStatus function
     *
     * @param string $function The function to be run.
     * @param string $workload Data for the function to operate on.
     * @param int $priority One of the JobPriority constants.
     * @param string $unique A unique ID for the job.
     *
     * @return PromiseInterface The job handle assigned.
     */
    public function submitBackgroundJob(string $function, string $workload, int $priority = JobPriority::NORMAL, string $unique = '') : PromiseInterface{
        $jobDetails = new ClientJobData($function, $workload, $unique, $priority);
        $jobDetails->background = true;

        return $this->connect()->then(function(Endpoint $server) use ($jobDetails){
            return $this->createJob($server, $jobDetails);
        })->then(function() use ($jobDetails){
            return new BackgroundJob($jobDetails, $this);
        });
    }

    /**
     * Submit a job status request to determine the status of a background job.
     * You must wait for the status response by calling the wait method.
     *
     * @param string $handle The handle of the job to get status information for.
     *
     * @return PromiseInterface
     */
    public function getJobStatus(string $handle) : PromiseInterface{
        $data = new JobStatusData($handle);

        return $this->connect()->then(function(Endpoint $server) use ($data){
            return (new JobStatusHandler($data, $this->logger))->waitForResult($server);
        })->then(function() use ($data){
            return new JobStatus($data);
        });
    }

    public function disconnect() : void{
        foreach ($this->serverList as $server){
            $server->disconnect();
        }
    }

    /**
     * Register a function with the server.
     *
     * @param string $name The name of the function.
     * @param callable $callback A callback to be executed when a job is received.
     * @param int|null $timeout A time limit on how the server should wait for a response.
     *
     * @return PromiseInterface
     */
    public function registerFunction(string $name, callable $callback, int $timeout = null) : self{
        $this->functionList->register(new WorkerFunction($name, $callback, $timeout));
        if ($timeout === null){
            $packet = new BinaryPacket(PacketMagic::REQ, PacketType::CAN_DO, [$name]);
        } else {
            $packet = new BinaryPacket(PacketMagic::REQ, PacketType::CAN_DO_TIMEOUT, [$name, $timeout]);
        }

        $this->connect(true)->done(function(array $connectedServers) use ($packet){
            foreach ($connectedServers as $server){
                $server->writePacket($packet);
            }
        });

        return $this;
    }

    /**
     * Go into a loop accepting jobs and performing the work.
     */
    public function work() : void{
        if (empty($this->functionList)){
            throw new NoRegisteredFunctionException;
        }

        $this->workAsync()->done();
        $this->loop->run();
    }

    /**
     * Begin the process of accepting jobs while allowing the main script to continue.
     * Main script must run the main loop at some future point.
     *
     */
    public function workAsync() : PromiseInterface{
        if ($this->stopWorking){
            return reject(new \GearmanException('Worker stopped'));
        }

        $grabHandler = new GrabJobHandler($this->logger);
        $sleepHandler = new SleepHandler($this->logger);

        return $this->connect(true)->then(function(array $serverList) use ($grabHandler, $sleepHandler){
            $grabJob = function() use (&$serverList, &$grabJob, $grabHandler, $sleepHandler){
                $server = current($serverList) ?: reset($serverList);

                return $grabHandler->grabJob($server)->then(function(WorkerJob $job){
                    $this->functionList->run($job);
                }, function($error) use ($sleepHandler, $server){
                    if (!$error instanceof NoWorkException){
                        throw $error;
                    }

                    return $sleepHandler->sleep($server);
                })->done($grabJob);
            };

            $grabJob();
        });
    }

    /**
     * Stop accepting new jobs. Any job currently in progress will be completed.
     */
    public function stopWorking() : void{
        $this->stopWorking = true;
    }

    /**
     * @param Endpoint $server
     * @param ClientJobData $jobDetails
     *
     * @return PromiseInterface
     */
    private function createJob(Endpoint $server, ClientJobData $jobDetails) : PromiseInterface{
        return (new CreateJobHandler($jobDetails, $this->logger))->createJob($server);
    }

    private function connect(bool $all = false) : PromiseInterface{
        if (!$this->serverList){
            throw new EmptyServerListException();
        }

        $promiseList = [];
        foreach ($this->serverList as $server){
            $promiseList[] = $server->connect($this->autoDisconnect)->then(null, function($error) use ($all){
                if ($all){
                    return null;
                } else {
                    throw $error;
                }
            });
        }

        if ($all){
            return all($promiseList)->then(function(array $connectedEndpoints){
                return array_filter($connectedEndpoints);
            });
        } else {
            return any($promiseList)->then(function(Endpoint $endpoint){
                foreach ($this->serverList as $item){
                    if ($item !== $endpoint){
                        $item->disconnect();
                    }
                }

                return $endpoint;
            });
        }
    }
}
