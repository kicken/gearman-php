<?php

namespace Kicken\Gearman\Job\Data;

class JobStatusData extends JobData {
    public bool $isKnown = false;
    public bool $isRunning = false;
}