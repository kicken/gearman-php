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

namespace Kicken\Gearman\Job;

use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;

/**
 * Provides information regarding a job to worker functions.
 *
 * @package Kicken\Gearman\Job
 */
class WorkerJob {
    private $jobDetails;
    private $resultSent = false;
    private $timeout = false;

    public function __construct(JobDetails $details, $timeout = false){
        $this->jobDetails = $details;
        $this->setTimeout($timeout);
    }

    public function getJobHandle(){
        return $this->jobDetails->jobHandle;
    }

    public function getWorkload(){
        return $this->jobDetails->workload;
    }

    public function getUniqueId(){
        return $this->jobDetails->unique;
    }

    public function getFunction(){
        return $this->jobDetails->function;
    }

    /**
     * Send a data packet back to the client
     *
     * @param $data
     */
    public function sendData($data){
        $packet = new Packet(PacketMagic::REQ, PacketType::WORK_DATA, [$this->jobDetails->jobHandle, $data]);

        $this->send($packet);
    }

    /**
     * Send progress status information back to the client.
     *
     * @param $numerator
     * @param $denominator
     */
    public function sendStatus($numerator, $denominator){
        $packet = new Packet(PacketMagic::REQ, PacketType::WORK_STATUS, [$this->jobDetails->jobHandle, $numerator, $denominator]);
        $this->send($packet);
    }

    /**
     * Send warning information back to the client
     *
     * @param $data
     */
    public function sendWarning($data){
        $packet = new Packet(PacketMagic::REQ, PacketType::WORK_WARNING, [$this->jobDetails->jobHandle, $data]);
        $this->send($packet);
    }

    /**
     * Send final data back to the client and mark the job as complete.
     *
     * @param string $data
     */
    public function sendComplete($data = ''){
        if (!$this->resultSent){
            $packet = new Packet(PacketMagic::REQ, PacketType::WORK_COMPLETE, [$this->jobDetails->jobHandle, $data]);
            $this->send($packet);
            $this->resultSent = true;
        }
    }

    /**
     * Mark the job as failed.
     */
    public function sendFail(){
        if (!$this->resultSent){
            $packet = new Packet(PacketMagic::REQ, PacketType::WORK_FAIL, [$this->jobDetails->jobHandle]);
            $this->send($packet);
            $this->resultSent = true;
        }
    }

    /**
     * Send an exception back to the client and mark the job as failed.
     *
     * @param $exception
     */
    public function sendException($exception){
        if (!$this->resultSent){
            $packet = new Packet(PacketMagic::REQ, PacketType::WORK_EXCEPTION, [$this->jobDetails->jobHandle, $exception]);
            $this->send($packet);
            $this->sendFail();
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

    private function send(Packet $packet){
        $this->jobDetails->connection->writePacket($packet, $this->timeout);
    }
}
