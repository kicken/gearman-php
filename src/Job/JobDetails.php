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


use Kicken\Gearman\Client;
use Kicken\Gearman\Protocol\Connection;

/**
 * Used to share data between the Client and ClientJob classes.
 *
 * @package Kicken\Gearman\Job
 */
class JobDetails {
    public $jobHandle;
    public $unique;
    public $function;
    public $background = false;
    public $finished = false;
    public $priority;
    public $workload;
    public $numerator = 0;
    public $denominator = 0;
    public $result;
    public $data;
    /**
     * @var Client
     */
    public $client;

    /**
     * @var Connection
     */
    public $connection;
    public $callbacks = [];

    public function __construct($function, $workload, $unique, $priority){
        $this->function = $function;
        $this->workload = $workload;
        $this->unique = $unique?:uniqid();
        $this->priority = $priority;
    }

    public function addCallback($type, callable $fn){
        $this->callbacks[$type][] = $fn;
    }

    public function triggerCallback($type){
        if (isset($this->callbacks[$type])){
            foreach ($this->callbacks[$type] as $fn){
                call_user_func($fn);
            }
        }
    }
}
