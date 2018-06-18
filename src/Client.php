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
use Kicken\Gearman\Job\ClientJob;
use Kicken\Gearman\Job\JobDetails;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Protocol\Connection;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Status\JobStatus;
use Kicken\Gearman\Status\StatusDetails;

/**
 * A class for submitting jobs to a Gearman server.
 *
 * @package Kicken\Gearman
 */
class Client {
    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var JobDetails
     */
    private $newJobDetails;

    /**
     * @var JobDetails[]
     */
    private $jobList = [];

    /**
     * @var StatusDetails[]
     */
    private $statusList = [];

    /**
     * @var int|bool
     */
    private $timeout = false;

    /**
     * Create a new Gearman Client, used for submitting new jobs or checking the status of existing jobs.
     *
     * @param string|array|Connection $connection The server(s) to connect to.
     */
    public function __construct($connection = '127.0.0.1:4730'){
        if (!($connection instanceof Connection)){
            if (!is_array($connection)){
                $connection = [$connection];
            }
            $connection = new Connection($connection);
        }

        $this->connection = $connection;
    }

    /**
     *
     * Submit a new job to the server.  Results can be retrieved once the job is complete through the returned
     * ClientJob object.  Use the wait method to wait for the job to complete.
     *
     * @param string $function The function to be run.
     * @param string $workload Data for the function to operate on.
     * @param int $priority One of the JobPriority constants.
     * @param string $unique A unique ID for the job.
     *
     * @return ClientJob
     * @see wait
     */
    public function submitJob($function, $workload, $priority = JobPriority::NORMAL, $unique = null){
        if (!is_scalar($workload)){
            throw new \InvalidArgumentException('Workload can only be of a scalar type (string, integer, etc.)');
        }

        $jobDetails = $this->createJobDetails($function, $workload, $unique, $priority);
        $job = new ClientJob($jobDetails);
        $packetType = $this->getSubmitJobType($priority, false);
        $arguments = [$jobDetails->function, $jobDetails->unique, $jobDetails->workload];

        $this->newJobDetails = $jobDetails;

        $packet = new Packet(PacketMagic::REQ, $packetType, $arguments);
        $this->connection->writePacket($packet);
        while ($this->newJobDetails->jobHandle === null){
            $this->packetIteration();
        }

        $this->newJobDetails = null;

        return $job;
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
     * @return string The job handle assigned.
     */
    public function submitBackgroundJob($function, $workload, $priority = JobPriority::NORMAL, $unique = null){
        if (!is_scalar($workload)){
            throw new \InvalidArgumentException('Workload can only be of a scalar type (string, integer, etc.)');
        }

        $jobDetails = $this->createJobDetails($function, $workload, $unique, $priority);
        $jobDetails->background = true;
        $packetType = $this->getSubmitJobType($priority, true);
        $arguments = [$jobDetails->function, $jobDetails->unique, $jobDetails->workload];

        $this->newJobDetails = $jobDetails;

        $packet = new Packet(PacketMagic::REQ, $packetType, $arguments);
        $this->connection->writePacket($packet);
        while ($this->newJobDetails->jobHandle === null){
            $this->packetIteration();
        }

        $this->newJobDetails = null;

        return $jobDetails->jobHandle;
    }

    /**
     * Submit a job status request to determine the status of a background job.
     * You must wait for the status response by calling the wait method.
     *
     * @param string $handle The handle of the job to get status information for.
     *
     * @return JobStatus
     */
    public function getJobStatus($handle){
        $statusDetails = $this->createStatusDetails($handle);
        $status = new JobStatus($statusDetails);

        $this->statusList[] = $statusDetails;

        $packet = new Packet(PacketMagic::REQ, PacketType::GET_STATUS, [$handle]);
        $this->connection->writePacket($packet);

        return $status;
    }

    /**
     * Wait jobs or status requests to complete.
     *
     * @param ClientJob|JobStatus $for A specific job or status request to wait for.
     */
    public function wait($for = null){
        while ($this->workToBeDone($for)){
            $this->packetIteration();
        }
    }

    /**
     * Configure a timeout when waiting for foreground job results.
     *
     * @param int|bool $timeout Timeout in milliseconds or false for no timeout
     */
    public function setTimeout($timeout){
        if ($timeout === true){
            $timeout = ini_get('default_socket_timeout');
        } else if ($timeout === -1){
            $timeout = false;
        }

        if ($timeout < 0){
            throw new \InvalidArgumentException('Timeout must be a positive integer or false.');
        }


        $this->timeout = $timeout;
    }

    private function packetIteration(){
        $packet = $this->connection->readPacket($this->timeout);
        $this->processPacket($packet);
    }

    private function workToBeDone($job = null){
        if ($job instanceof ClientJob){
            return !$job->isFinished();
        } else if ($job instanceof JobStatus){
            return !$job->isResultReceived();
        } else {
            $count = 0;
            foreach ($this->jobList as $details){
                $countThisOne = !$details->background && !$details->finished;
                $count += $countThisOne?1:0;
            }

            foreach ($this->statusList as $details){
                $count += $details->resultReceived?0:1;
            }

            return $count > 0;
        }
    }

    private function createJobDetails($function, $workload, $unique, $priority){
        $jobDetails = new JobDetails($function, $workload, $unique, $priority);
        $jobDetails->client = $this;
        $jobDetails->connection = $this->connection;

        return $jobDetails;
    }

    private function createStatusDetails($handle){
        $details = new StatusDetails();
        $details->jobHandle = $handle;
        $details->client = $this;

        return $details;
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
            $this->newJobDetails->jobHandle = $handle;
            $this->jobList[$handle] = $this->newJobDetails;
        } else {
            if (!$this->jobList[$handle]){
                return;
            }

            $job = $this->jobList[$handle];
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
                    $job->result = $job->result . $job->data;
                    $job->finished = true;
                    $job->triggerCallback($packetType == PacketType::WORK_COMPLETE?'complete':'warning');
                    break;
                case PacketType::WORK_FAIL:
                    $job->finished = true;
                    $job->triggerCallback('fail');
                    break;
                case PacketType::WORK_DATA:
                    $job->data = $packet->getArgument(1);
                    $job->result = $job->result . $job->data;
                    $job->triggerCallback('data');
                    break;
            }
        }
    }

    private function updateStatusDetails(Packet $packet){
        $handle = $packet->getArgument(0);
        foreach ($this->statusList as $statusDetails){
            if ($statusDetails->jobHandle === $handle){
                $statusDetails->isKnown = (bool)(int)$packet->getArgument(1);
                $statusDetails->isRunning = (bool)(int)$packet->getArgument(2);
                $statusDetails->numerator = (int)$packet->getArgument(3);
                $statusDetails->denominator = (int)$packet->getArgument(4);
                $statusDetails->resultReceived = true;
                $statusDetails->triggerCallback('complete');
            }
        }
    }

    private function getSubmitJobType($priority, $background){
        switch ($priority){
            case JobPriority::HIGH:
                return $background?PacketType::SUBMIT_JOB_HIGH_BG:PacketType::SUBMIT_JOB_HIGH;
            case JobPriority::NORMAL:
                return $background?PacketType::SUBMIT_JOB_BG:PacketType::SUBMIT_JOB;
            case JobPriority::LOW:
                return $background?PacketType::SUBMIT_JOB_LOW_BG:PacketType::SUBMIT_JOB_LOW;
            default:
                throw new \InvalidArgumentException('Invalid job priority');
        }
    }
}
