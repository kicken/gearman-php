<?php

namespace Kicken\Gearman\Job\Data;

class JobData {
    public ?string $jobHandle = null;
    public int $numerator = 0;
    public int $denominator = 0;

    public function __construct(?string $jobHandle){
        $this->jobHandle = $jobHandle;
    }
}
