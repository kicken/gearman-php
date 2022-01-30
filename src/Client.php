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
use Kicken\Gearman\Job\Data\ClientJobData;
use Kicken\Gearman\Job\Data\JobData;
use Kicken\Gearman\Job\Data\JobStatusData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Job\JobStatus;
use Kicken\Gearman\Network\Server;
use Kicken\Gearman\Network\ServerPool;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
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
    /** @var ClientJobData[] */
    private array $jobList = [];
    /** @var JobStatusData[] */
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
     * @return PromiseInterface<ClientJob>
     */
    public function submitJob(string $function, string $workload, int $priority = JobPriority::NORMAL, string $unique = null) : PromiseInterface{
        $jobDetails = new ClientJobData($function, $workload, $unique ?? uniqid(), $priority);

        return $this->doSubmitJob($jobDetails);
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
     * @return PromiseInterface<Clientjob> The job handle assigned.
     */
    public function submitBackgroundJob(string $function, string $workload, int $priority = JobPriority::NORMAL, string $unique = null) : PromiseInterface{
        $jobDetails = new ClientJobData($function, $workload, $unique ?? uniqid(), $priority);
        $jobDetails->background = true;

        return $this->doSubmitJob($jobDetails);
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
            $status = new JobStatusData($handle);
            $this->statusList[] = $status;

            $packet = new Packet(PacketMagic::REQ, PacketType::GET_STATUS, [$handle]);
            $server = $this->serverPool->randomServer();
            $server->writePacket($packet);

            return new Promise(function($resolve) use ($status){
                $loop = function() use ($status, $resolve, &$loop){
                    if ($status->responseReceived){
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
     * @param ClientJobData $jobDetails
     *
     * @return PromiseInterface<ClientJob>
     */
    private function doSubmitJob(ClientJobData $jobDetails) : PromiseInterface{
        return $this->connect()->then(function() use ($jobDetails){
            $packetType = $this->getSubmitJobType($jobDetails->priority, $jobDetails->background);
            $arguments = [$jobDetails->function, $jobDetails->unique, $jobDetails->workload];

            $packet = new Packet(PacketMagic::REQ, $packetType, $arguments);
            $server = $this->serverPool->randomServer();
            $server->writePacket($packet);
            $this->jobList[] = $jobDetails;

            return new Promise(function(callable $resolve) use ($jobDetails){
                $checkForHandle = function() use ($jobDetails, $resolve, &$checkForHandle){
                    if ($jobDetails->jobHandle){
                        if ($jobDetails->background){
                            $resolve(new ClientBackgroundJob($this, $jobDetails));
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

    private function packetReceived(Packet $packet){
        $this->processPacket($packet);
        $this->loop->futureTick(function(){
            if (!$this->statusList && !$this->jobList){
                $this->serverPool->disconnect();
            }
        });
    }

    private function processPacket(Packet $packet){
        $handle = $packet->getArgument(0);
        switch ($packet->getType()){
            case PacketType::JOB_CREATED:
                $lastJob = array_pop($this->jobList);
                $lastJob->jobHandle = $handle;
                if (!$lastJob->background){
                    $this->jobList[] = $lastJob;
                }
                break;
            case PacketType::WORK_DATA:
            case PacketType::WORK_WARNING:
            case PacketType::WORK_STATUS:
            case PacketType::WORK_COMPLETE:
            case PacketType::WORK_FAIL:
            case PacketType::WORK_EXCEPTION:
                if ($data = $this->findHandleInList($this->jobList, $handle)){
                    $this->updateWorkJobData($packet, ...$data);
                }
                break;
            case PacketType::STATUS_RES:
                if ($data = $this->findHandleInList($this->statusList, $handle)){
                    $this->UpdateJobStatusData($packet, ...$data);
                }
                break;
            case PacketType::ERROR:
                throw new ErrorException($packet);
        }
    }

    private function updateWorkJobData(Packet $packet, int $index, ClientJobData $data){
        switch ($packet->getType()){
            case PacketType::WORK_STATUS:
                $data->numerator = (int)$packet->getArgument(1);
                $data->denominator = (int)$packet->getArgument(2);
                $data->triggerCallback('status');
                break;
            case PacketType::WORK_WARNING:
                $data->data = $packet->getArgument(1);
                $data->triggerCallback('warning');
                break;
            case PacketType::WORK_COMPLETE:
            case PacketType::WORK_EXCEPTION:
                $data->data = $packet->getArgument(1);
                $data->result = $data->data;
                $data->finished = true;
                $data->triggerCallback($packet->getType() == PacketType::WORK_COMPLETE ? 'complete' : 'warning');
                unset($this->jobList[$index]);
                break;
            case PacketType::WORK_FAIL:
                $data->finished = true;
                $data->triggerCallback('fail');
                unset($this->jobList[$index]);
                break;
            case PacketType::WORK_DATA:
                $data->data = $packet->getArgument(1);
                $data->triggerCallback('data');
                break;
        }
    }

    private function UpdateJobStatusData(Packet $packet, int $index, JobStatusData $data){
        $data->isKnown = (bool)(int)$packet->getArgument(1);
        $data->isRunning = (bool)(int)$packet->getArgument(2);
        $data->numerator = (int)$packet->getArgument(3);
        $data->denominator = (int)$packet->getArgument(4);
        $data->responseReceived = true;
        unset($this->statusList[$index]);
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
                            $this->packetReceived($packet);
                        });
                    }
                });

                $this->loop->futureTick($resolveIfConnected);
            }
        });
    }

    private function findHandleInList(array $list, string $handle) : ?array{
        /**
         * @var int $index
         * @var JobData $details
         */
        foreach ($list as $index => $details){
            if ($details->jobHandle === $handle){
                return [$index, $details];
            }
        }

        return null;
    }
}
