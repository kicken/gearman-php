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

use Kicken\Gearman\Exception\CouldNotConnectException;
use Kicken\Gearman\Exception\EmptyServerListException;
use Kicken\Gearman\Job\ClientBackgroundJob;
use Kicken\Gearman\Job\ClientForegroundJob;
use Kicken\Gearman\Job\Data\ClientJobData;
use Kicken\Gearman\Job\Data\JobStatusData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Job\JobStatus;
use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\CreateJobHandler;
use Kicken\Gearman\Network\PacketHandler\JobStatusHandler;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Promise\ExtendedPromiseInterface;
use function React\Promise\resolve;

/**
 * A class for submitting jobs to a Gearman server.
 *
 * @package Kicken\Gearman
 */
class Client {
    /** @var Endpoint[] */
    private array $serverList;

    private ?Connection $connectedServer = null;
    private ?ExtendedPromiseInterface $pendingServerAttempt = null;

    /**
     * Create a new Gearman Client, used for submitting new jobs or checking the status of existing jobs.
     *
     * @param string|string[]|Endpoint|Endpoint[] $serverList The server(s) to connect to.
     */
    public function __construct($serverList = '127.0.0.1:4730', LoopInterface $loop = null){
        $this->serverList = mapToEndpointObjects($serverList, $loop ?? Loop::get());
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
     * @return ExtendedPromiseInterface
     */
    public function submitJob(string $function, string $workload, int $priority = JobPriority::NORMAL, string $unique = '') : ExtendedPromiseInterface{
        $jobDetails = new ClientJobData($function, $workload, $unique, $priority);

        /** @noinspection PhpIncompatibleReturnTypeInspection */
        return $this->connect()->then(function(Connection $server) use ($jobDetails){
            return $this->createJob($server, $jobDetails);
        })->then(function() use ($jobDetails){
            return new ClientForegroundJob($jobDetails);
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
     * @return ExtendedPromiseInterface The job handle assigned.
     */
    public function submitBackgroundJob(string $function, string $workload, int $priority = JobPriority::NORMAL, string $unique = '') : ExtendedPromiseInterface{
        $jobDetails = new ClientJobData($function, $workload, $unique, $priority);
        $jobDetails->background = true;

        /** @noinspection PhpIncompatibleReturnTypeInspection */
        return $this->connect()->then(function(Connection $server) use ($jobDetails){
            return $this->createJob($server, $jobDetails);
        })->then(function() use ($jobDetails){
            return new ClientBackgroundJob($jobDetails, $this);
        });
    }

    /**
     * Submit a job status request to determine the status of a background job.
     * You must wait for the status response by calling the wait method.
     *
     * @param string $handle The handle of the job to get status information for.
     *
     * @return ExtendedPromiseInterface
     */
    public function getJobStatus(string $handle) : ExtendedPromiseInterface{
        $data = new JobStatusData($handle);

        /** @noinspection PhpIncompatibleReturnTypeInspection */
        return $this->connect()->then(function(Connection $server) use ($data){
            return (new JobStatusHandler($data))->waitForResult($server);
        })->then(function() use ($data){
            return new JobStatus($data);
        });
    }

    /**
     * @param Connection $server
     * @param ClientJobData $jobDetails
     *
     * @return ExtendedPromiseInterface
     */
    private function createJob(Connection $server, ClientJobData $jobDetails) : ExtendedPromiseInterface{
        return (new CreateJobHandler($jobDetails))->createJob($server);
    }

    private function connect() : ExtendedPromiseInterface{
        if ($this->connectedServer && $this->connectedServer->isConnected()){
            return resolve($this->connectedServer);
        } else if ($this->pendingServerAttempt){
            return $this->pendingServerAttempt;
        } else if (!$this->serverList){
            throw new EmptyServerListException();
        }

        /** @var Endpoint[] $queue */
        $queue = $this->serverList;
        $firstServer = array_shift($queue);
        $failureHandler = function() use (&$queue, &$failureHandler){
            $nextServer = array_shift($queue);
            if (!$nextServer){
                throw new CouldNotConnectException();
            }

            return $nextServer->connect()->otherwise($failureHandler);
        };


        return $this->pendingServerAttempt = $firstServer->connect()->otherwise($failureHandler)->then(function(Connection $server){
            $this->connectedServer = $server;
            $this->pendingServerAttempt = null;

            return $server;
        });
    }
}
