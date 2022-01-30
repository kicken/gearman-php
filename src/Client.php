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

use Kicken\Gearman\Job\ClientBackgroundJob;
use Kicken\Gearman\Job\ClientForegroundJob;
use Kicken\Gearman\Job\ClientJob;
use Kicken\Gearman\Job\Data\ClientJobData;
use Kicken\Gearman\Job\Data\JobStatusData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Job\JobStatus;
use Kicken\Gearman\Network\PacketHandler\CreateJobHandler;
use Kicken\Gearman\Network\PacketHandler\JobStatusHandler;
use Kicken\Gearman\Network\ServerPool;
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

        return $this->connect()->then(function() use ($jobDetails){
            return $this->createJob($jobDetails);
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
     * @param ?string $unique A unique ID for the job.
     *
     * @return PromiseInterface<Clientjob> The job handle assigned.
     */
    public function submitBackgroundJob(string $function, string $workload, int $priority = JobPriority::NORMAL, string $unique = null) : PromiseInterface{
        $jobDetails = new ClientJobData($function, $workload, $unique ?? uniqid(), $priority);
        $jobDetails->background = true;

        return $this->connect()->then(function() use ($jobDetails){
            return $this->createJob($jobDetails);
        })->then(function() use ($jobDetails){
            return new ClientBackgroundJob($this, $jobDetails);
        });
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
        $data = new JobStatusData($handle);

        return $this->connect()->then(function() use ($data){
            $server = $this->serverPool->randomServer();

            return (new JobStatusHandler($data))->waitForResult($server);
        })->then(function() use ($data){
            return new JobStatus($data);
        });
    }

    /**
     * @param ClientJobData $jobDetails
     *
     * @return PromiseInterface<ClientJob>
     */
    private function createJob(ClientJobData $jobDetails) : PromiseInterface{
        $server = $this->serverPool->randomServer();

        return (new CreateJobHandler($jobDetails))->createJob($server);
    }

    private function connect() : PromiseInterface{
        return new Promise(function($resolve){
            if ($this->serverPool->isConnected()){
                $resolve();
            } else {
                $this->serverPool->connect(function() use ($resolve){
                    $resolve();
                });
            }
        });
    }
}
