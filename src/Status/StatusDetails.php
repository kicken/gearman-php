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

namespace Kicken\Gearman\Status;


use Kicken\Gearman\Client;

/**
 * Used to share data between a returned JobStatus object and the gearman client object.
 *
 * @package Kicken\Gearman\Status
 */
class StatusDetails {
    public $resultReceived = false;
    public $jobHandle;
    public $isKnown = false;
    public $isRunning = false;
    public $numerator = 0;
    public $denominator = 0;
    /**
     * @var Client
     */
    public $client;
    /**
     * @var callable[]
     */
    public $callbacks = [];

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
