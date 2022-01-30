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

use Kicken\Gearman\Job\Data\ClientJobData;
use Kicken\Gearman\Job\Data\JobData;
use Kicken\Gearman\Job\Data\WorkJobData;

/**
 * Provides information regarding a job to a client.
 *
 * @package Kicken\Gearman\Job
 */
abstract class ClientJob extends Job {
    /** @var JobData|ClientJobData */
    protected JobData $data;

    public function __construct(ClientJobData $jobDetails){
        $this->data = $jobDetails;
    }

    public function getUniqueId() : string{
        return $this->data->unique;
    }

    public function getFunction() : string{
        return $this->data->function;
    }

    public function isFinished() : bool{
        return $this->data->finished;
    }

    public function getPriority() : int{
        return $this->data->priority;
    }

    public function getWorkload() : string{
        return $this->data->workload;
    }

    public function getResult() : string{
        return $this->data->result;
    }

    public function getData() : string{
        return $this->data->data;
    }
}
