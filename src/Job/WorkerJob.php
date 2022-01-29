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

use Kicken\Gearman\Network\Server;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;

/**
 * Provides information regarding a job to worker functions.
 *
 * @package Kicken\Gearman\Job
 */
class WorkerJob {
    private Server $server;
    private JobDetails $jobDetails;
    private bool $resultSent = false;

    public function __construct(Server $server, JobDetails $details){
        $this->server = $server;
        $this->jobDetails = $details;
    }

    public function getJobHandle():string{
        return $this->jobDetails->jobHandle;
    }

    public function getWorkload():string{
        return $this->jobDetails->workload;
    }

    public function getUniqueId():string{
        return $this->jobDetails->unique;
    }

    public function getFunction():string{
        return $this->jobDetails->function;
    }

    /**
     * Send a data packet back to the client
     *
     * @param $data
     */
    public function sendData($data) : void{
        $packet = new Packet(PacketMagic::REQ, PacketType::WORK_DATA, [$this->jobDetails->jobHandle, $data]);

        $this->send($packet);
    }

    /**
     * Send progress status information back to the client.
     *
     * @param $numerator
     * @param $denominator
     */
    public function sendStatus($numerator, $denominator) : void{
        $packet = new Packet(PacketMagic::REQ, PacketType::WORK_STATUS, [$this->jobDetails->jobHandle, $numerator, $denominator]);
        $this->send($packet);
    }

    /**
     * Send warning information back to the client
     *
     * @param $data
     */
    public function sendWarning($data) : void{
        $packet = new Packet(PacketMagic::REQ, PacketType::WORK_WARNING, [$this->jobDetails->jobHandle, $data]);
        $this->send($packet);
    }

    /**
     * Send final data back to the client and mark the job as complete.
     *
     * @param string $data
     */
    public function sendComplete(string $data = '') : void{
        if (!$this->resultSent){
            $packet = new Packet(PacketMagic::REQ, PacketType::WORK_COMPLETE, [$this->jobDetails->jobHandle, $data]);
            $this->send($packet);
            $this->resultSent = true;
        }
    }

    /**
     * Mark the job as failed.
     */
    public function sendFail() : void{
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
    public function sendException($exception) : void{
        if (!$this->resultSent){
            $packet = new Packet(PacketMagic::REQ, PacketType::WORK_EXCEPTION, [$this->jobDetails->jobHandle, $exception]);
            $this->send($packet);
            $this->sendFail();
        }
    }

    private function send(Packet $packet) : void{
        $this->server->writePacket($packet);
    }
}
