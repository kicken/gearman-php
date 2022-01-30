<?php

namespace Kicken\Gearman\Job;

use Kicken\Gearman\Job\Data\JobData;

abstract class Job {
    protected JobData $data;

    public function getJobHandle() : string{
        return $this->data->jobHandle;
    }

    public function getNumerator() : int{
        return $this->data->numerator;
    }

    public function getDenominator() : int{
        return $this->data->denominator;
    }

    public function getProgressPercentage() : float{
        if ($this->data->denominator === 0){
            return 0;
        }

        return $this->data->numerator / $this->data->denominator;
    }
}