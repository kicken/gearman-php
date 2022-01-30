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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

namespace Kicken\Gearman;

use Kicken\Gearman\Exception\ErrorException;
use Kicken\Gearman\Job\ClientBackgroundJob;
use Kicken\Gearman\Job\ClientForegroundJob;
use Kicken\Gearman\Job\ClientJob;
use Kicken\Gearman\Job\JobDetails;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Network\Server;
use Kicken\Gearman\Network\ServerPool;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Status\JobStatus;
use Kicken\Gearman\Status\StatusDetails;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Promise\Promise;
use React\Promise\PromiseInterface;

/**
 * A class for submitting jobs to a Gearman server.
 *
 * @package Kicken\Gearman
 */
class Client {
    private LoopInterface $loop;
    private ServerPool $serverPool;
    /** @var JobDetails[] */
    private array $jobList = [];
    /** @var StatusDetails[] */
    private array $statusList = [];

    /**
     * Create a new Gearman Client, used for submitting new jobs or checking the status of existing jobs.
     *
     * @param string|string[] $serverList The server(s) to connect to.
     */
    public function __construct($serverList = '127.0.0.1:4730', int $connectTimeout = null, LoopInterface $loop = null){
        if (!is_array($serverList)){
            $serverList = [$serverList];
        }

        $this->loop = $loop ?? Loop::get();
        $this->serverPool = new ServerPool($serverList, $connectTimeout ?? ini_get('default_socket_timeout'), $this->loop);
    }

    /**
     * Submit a new job to the server.  Results can be retrieved once the job is complete through the returned
     * ClientJob object.  Use the wait method to wait for the job to complete.
     *
     * @param string $function The function to be run.
     * @param string $workload Data for the function to operate on.
     * @param int $priority One of the JobPriority constants.
     * @param ?string $unique A unique ID for the job.
     *
     * @return Promise<ClientJob>
     * @see wait
     */
    public function submitJob(string $function, string $workload, int $priority = JobPriority::NORMAL, string $unique = null) : PromiseInterface{
        $jobDetails = new JobDetails($function, $workload, $unique, $priority);

        return $this->doSubmitJob($jobDetails, $priority);
    }

    /**
     * Submit a new job to the server for execution as a background task.  Background tasks are unable to pass back any
     * result data, but can provide status information regarding the progress of the job.  Status information must be
     * obtained by calling the getJobStatus function
     *
     * @param string $function The function to be run.
     * @param string $workload Data for the function to operate on.
     * @param int $priority One of the JobPriority constants.
     * @param ?string $unique A unique ID for the job.
     *
     * @return Promise<Clientjob> The job handle assigned.
     */
    public function submitBackgroundJob(string $function, string $workload, int $priority = JobPriority::NORMAL, string $unique = null) : PromiseInterface{
        $jobDetails = new JobDetails($function, $workload, $unique, $priority);
        $jobDetails->background = true;

        return $this->doSubmitJob($jobDetails, $priority);
    }

    /**
     * Submit a job status request to determine the status of a background job.
     * You must wait for the status response by calling the wait method.
     *
     * @param string $handle The handle of the job to get status information for.
     *
     * @return PromiseInterface<JobStatus>
     */
    public function getJobStatus(string $handle) : PromiseInterface{
        return $this->connect()->then(function() use ($handle){
            $status = new StatusDetails($handle);
            $this->statusList[] = $status;

            $packet = new Packet(PacketMagic::REQ, PacketType::GET_STATUS, [$handle]);
            $server = $this->serverPool->randomServer();
            $server->writePacket($packet);

            return new Promise(function($resolve) use ($status){
                $loop = function() use ($status, $resolve, &$loop){
                    if ($status->resultReceived){
                        $resolve(new JobStatus($status));
                    } else {
                        usleep(1000);
                        $this->loop->futureTick($loop);
                    }
                };
                $this->loop->futureTick($loop);
            });
        });
    }

    /**
     * @param int $priority
     * @param JobDetails $jobDetails
     *
     * @return Promise<ClientJob>
     */
    private function doSubmitJob(JobDetails $jobDetails, int $priority) : PromiseInterface{
        return $this->connect()->then(function() use ($jobDetails, $priority){
            $packetType = $this->getSubmitJobType($priority, $jobDetails->background);
            $arguments = [$jobDetails->function, $jobDetails->unique, $jobDetails->workload];

            $packet = new Packet(PacketMagic::REQ, $packetType, $arguments);
            $server = $this->serverPool->randomServer();
            $server->writePacket($packet);
            $this->jobList[] = $jobDetails;

            return new Promise(function(callable $resolve) use ($jobDetails){
                $checkForHandle = function() use ($jobDetails, $resolve, &$checkForHandle){
                    if ($jobDetails->jobHandle){
                        if ($jobDetails->background){
                            $resolve(new ClientBackgroundJob($jobDetails));
                        } else {
                            $resolve(new ClientForegroundJob($jobDetails));
                        }
                    } else {
                        usleep(1000);
                        $this->loop->futureTick($checkForHandle);
                    }
                };

                $this->loop->futureTick($checkForHandle);
            });
        });
    }

    private function processPacket(Packet $packet){
        switch ($packet->getType()){
            case PacketType::JOB_CREATED:
            case PacketType::WORK_DATA:
            case PacketType::WORK_WARNING:
            case PacketType::WORK_STATUS:
            case PacketType::WORK_COMPLETE:
            case PacketType::WORK_FAIL:
            case PacketType::WORK_EXCEPTION:
                $this->updateJobDetails($packet);
                break;
            case PacketType::STATUS_RES:
                $this->updateStatusDetails($packet);
                break;
            case PacketType::ERROR:
                throw new ErrorException($packet);
        }
    }

    private function updateJobDetails(Packet $packet){
        $packetType = $packet->getType();
        $handle = $packet->getArgument(0);
        if ($packetType === PacketType::JOB_CREATED){
            $lastJob = end($this->jobList);
            $lastJob->jobHandle = $handle;
        } else if ($job = $this->getJobByHandle($handle)){
            switch ($packetType){
                case PacketType::WORK_STATUS:
                    $job->numerator = (int)$packet->getArgument(1);
                    $job->denominator = (int)$packet->getArgument(2);
                    $job->triggerCallback('status');
                    break;
                case PacketType::WORK_WARNING:
                    $job->data = $packet->getArgument(1);
                    $job->triggerCallback('warning');
                    break;
                case PacketType::WORK_COMPLETE:
                case PacketType::WORK_EXCEPTION:
                    $job->data = $packet->getArgument(1);
                    $job->result = $job->data;
                    $job->finished = true;
                    $job->triggerCallback($packetType == PacketType::WORK_COMPLETE ? 'complete' : 'warning');
                    unset($this->jobList[$handle]);
                    break;
                case PacketType::WORK_FAIL:
                    $job->finished = true;
                    $job->triggerCallback('fail');
                    unset($this->jobList[$handle]);
                    break;
                case PacketType::WORK_DATA:
                    $job->data = $packet->getArgument(1);
                    $job->triggerCallback('data');
                    break;
            }
        }
    }

    private function updateStatusDetails(Packet $packet){
        $handle = $packet->getArgument(0);
        $found = false;
        $statusDetails = null;
        foreach ($this->statusList as $idx => $statusDetails){
            if ($statusDetails->jobHandle === $handle){
                unset($this->statusList[$idx]);
                $found = true;
                break;
            }
        }

        if (!$found){
            return;
        }

        $statusDetails->isKnown = (bool)(int)$packet->getArgument(1);
        $statusDetails->isRunning = (bool)(int)$packet->getArgument(2);
        $statusDetails->numerator = (int)$packet->getArgument(3);
        $statusDetails->denominator = (int)$packet->getArgument(4);
        $statusDetails->resultReceived = true;
    }

    private function getSubmitJobType(int $priority, bool $background) : int{
        switch ($priority){
            case JobPriority::HIGH:
                return $background ? PacketType::SUBMIT_JOB_HIGH_BG : PacketType::SUBMIT_JOB_HIGH;
            case JobPriority::NORMAL:
                return $background ? PacketType::SUBMIT_JOB_BG : PacketType::SUBMIT_JOB;
            case JobPriority::LOW:
                return $background ? PacketType::SUBMIT_JOB_LOW_BG : PacketType::SUBMIT_JOB_LOW;
            default:
                throw new \InvalidArgumentException('Invalid job priority');
        }
    }

    private function connect() : PromiseInterface{
        return new Promise(function($resolve){
            $resolveIfConnected = function() use (&$resolveIfConnected, $resolve){
                if ($this->serverPool->isConnected()){
                    $resolve();
                } else {
                    sleep(1);
                    $this->loop->futureTick($resolveIfConnected);
                }
            };

            if ($this->serverPool->isConnected()){
                $resolve();
            } else {
                $this->serverPool->connect(function(array $serverList) use ($resolve){
                    foreach ($serverList as $server){
                        $server->onPacketReceived(function(Server $server, Packet $packet){
                            $this->processPacket($packet);
                        });
                    }
                });

                $this->loop->futureTick($resolveIfConnected);
            }
        });
    }

    private function getJobByHandle($handle) : ?JobDetails{
        foreach ($this->jobList as $details){
            if ($details->jobHandle === $handle){
                return $details;
            }
        }

        return null;
    }
}
