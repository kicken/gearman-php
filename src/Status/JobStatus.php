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

/**
 * Provides access to status information regarding a job.
 *
 * @param StatusDetails $details
 */
class JobStatus {
    /**
     * @var StatusDetails
     */
    private StatusDetails $statusDetails;


    public function __construct(StatusDetails $details){
        $this->statusDetails = $details;
    }

    public function isResultReceived() : bool{
        return $this->statusDetails->resultReceived;
    }

    public function getJobHandle() : string{
        return $this->statusDetails->jobHandle;
    }

    public function isKnown() : bool{
        return $this->statusDetails->isKnown;
    }

    public function isRunning() : bool{
        return $this->statusDetails->isRunning;
    }

    public function getNumerator() : int{
        return $this->statusDetails->numerator;
    }

    public function getDenominator() : int{
        return $this->statusDetails->denominator;
    }

    public function getProgressPercentage(){
        if ($this->statusDetails->denominator === 0){
            return 0;
        }

        return $this->statusDetails->numerator / $this->statusDetails->denominator;
    }

    public function onComplete(callable $fn){
        $wrapper = function() use ($fn){
            return call_user_func($fn, $this);
        };

        $this->statusDetails->addCallback('complete', $wrapper);
    }
}
