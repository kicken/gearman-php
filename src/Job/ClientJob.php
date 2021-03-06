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

/**
 * Provides information regarding a job to a client.
 *
 * @package Kicken\Gearman\Job
 */
class ClientJob {
    private $jobDetails;

    public function __construct(JobDetails $jobDetails){
        $this->jobDetails = $jobDetails;
    }

    public function getJobHandle(){
        return $this->jobDetails->jobHandle;
    }

    public function getUniqueId(){
        return $this->jobDetails->unique;
    }

    public function getFunction(){
        return $this->jobDetails->function;
    }

    public function isBackgroundJob(){
        return $this->jobDetails->background;
    }

    public function isFinished(){
        return $this->jobDetails->finished;
    }

    public function getPriority(){
        return $this->jobDetails->priority;
    }

    public function getWorkload(){
        return $this->jobDetails->workload;
    }

    public function getNumerator(){
        return $this->jobDetails->numerator;
    }

    public function getDenominator(){
        return $this->jobDetails->denominator;
    }

    public function getProgressPercentage(){
        return $this->jobDetails->numerator / $this->jobDetails->denominator;
    }

    public function getResult(){
        return $this->jobDetails->result;
    }

    public function getData(){
        return $this->jobDetails->data;
    }

    public function wait(){
        $this->jobDetails->client->wait($this);
    }

    public function onStatus(callable $fn){
        $this->addCallback('status', $fn);

        return $this;
    }

    public function onData(callable $fn){
        $this->addCallback('data', $fn);

        return $this;
    }

    public function onWarning(callable $fn){
        $this->addCallback('warning', $fn);

        return $this;
    }

    public function onComplete(callable $fn){
        $this->addCallback('complete', $fn);

        return $this;
    }

    public function onFail(callable $fn){
        $this->addCallback('fail', $fn);

        return $this;
    }

    public function onException(callable $fn){
        $this->addCallback('exception', $fn);

        return $this;
    }

    private function addCallback($type, $fn){
        $wrapper = function () use ($fn){
            return call_user_func($fn, $this);
        };

        $this->jobDetails->addCallback($type, $wrapper);
    }
}
