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


use Kicken\Gearman\Exception\LostConnectionException;
use Kicken\Gearman\Exception\NoRegisteredFunctionException;
use Kicken\Gearman\Job\JobDetails;
use Kicken\Gearman\Job\WorkerJob;
use Kicken\Gearman\Network\ServerPool;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

/**
 * A class for registering functions with and waiting for work from a Gearman server.
 *
 * @package Kicken\Gearman
 */
class Worker {
    /**
     * @var ServerPool
     */
    private $serverPool;

    /**
     * @var LoopInterface
     */
    private $loop;

    /**
     * @var callable[]
     */
    private $workerList = [];

    /**
     * @var bool
     */
    private $sleeping = false;

    /**
     * @var bool
     */
    private $stop = false;

    /**
     * @var int|bool
     */
    private $timeout = false;

    /**
     * Create a new Gearman Worker to process jobs submitted to the server by clients.
     *
     * @param string|string[] $serverList The server(s) to connect to.
     */
    public function __construct($serverList = '127.0.0.1:4730', LoopInterface $loop = null){
        if (!is_array($serverList)){
            $serverList = [$serverList];
        }

        $this->loop = $loop ?? Loop::get();
        $this->serverPool = new ServerPool($serverList, $this->loop);
    }

    /**
     * Register a function with the server.
     *
     * @param string $name The name of the function.
     * @param callable $callback A callback to be executed when a job is received.
     * @param int|null $timeout A time limit on how the server should wait for a response.
     *
     * @return $this
     */
    public function registerFunction($name, callable $callback, $timeout = null){
        $this->workerList[$name] = $callback;

        if ($timeout === null){
            $packet = new Packet(PacketMagic::REQ, PacketType::CAN_DO, [$name]);
        } else {
            $packet = new Packet(PacketMagic::REQ, PacketType::CAN_DO_TIMEOUT, [$name, $timeout]);
        }

        $this->serverPool->writePacket($packet, $this->timeout);

        return $this;
    }

    public function workOnce(){
        if (!$this->sleeping){
            $this->grabJob();
        }

        // Always process a new packet - this allows for new NOOP packets to
        // wake the worker back up.
        $packet = $this->serverPool->readPacket($this->timeout);
        $this->processPacket($packet);
    }

    /**
     * Go into a loop accepting jobs and performing the work.
     */
    public function work(){
        if (empty($this->workerList)){
            throw new NoRegisteredFunctionException;
        }

        $this->grabJob();
        $this->loop->run();
    }

    /**
     * Stop accepting new jobs. Any job currently in progress will be completed.
     */
    public function stopWorking(){
        $this->stop = true;
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

    private function grabJob(){
        $packet = new Packet(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ);
        $this->serverPool->writePacket($packet, $this->timeout);
    }

    private function sleep(){
        $packet = new Packet(PacketMagic::REQ, PacketType::PRE_SLEEP);
        $this->serverPool->writePacket($packet, $this->timeout);
        $this->sleeping = true;
    }

    private function processPacket(Packet $packet){
        switch ($packet->getType()){
            case PacketType::NO_JOB:
                $this->sleep();
                break;
            case PacketType::JOB_ASSIGN:
            case PacketType::JOB_ASSIGN_UNIQ:
                $this->processJob($packet);
                break;
            case PacketType::NOOP:
                $this->sleeping = false;
                break;
        }
    }

    private function processJob(Packet $packet){
        $workerName = $packet->getArgument(1);
        if (!isset($this->workerList[$workerName])){
            throw new NoRegisteredFunctionException;
        }

        $worker = $this->workerList[$workerName];
        $job = $this->createWorkerJob($packet);
        try {
            $result = call_user_func($worker, $job);
            if ($result === false){
                $job->sendFail();
            } else {
                $job->sendComplete((string)$result);
            }
        } catch (LostConnectionException $e){
            throw $e;
        } catch (\Exception $e){
            $job->sendException(get_class($e) . ': ' . $e->getMessage());
        }
    }

    private function createWorkerJob(Packet $packet){
        $jobDetails = $this->createJobDetails($packet);

        return new WorkerJob($jobDetails, $this->timeout);
    }

    private function createJobDetails(Packet $packet){
        if ($packet->getType() === PacketType::JOB_ASSIGN){
            $details = new JobDetails($packet->getArgument(1), $packet->getArgument(2), null, null);
        } else {
            $details = new JobDetails($packet->getArgument(1), $packet->getArgument(3), $packet->getArgument(2), null);
        }

        $details->jobHandle = $packet->getArgument(0);
        $details->connection = $this->serverPool;

        return $details;
    }
}
