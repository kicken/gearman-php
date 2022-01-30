<?php

namespace Kicken\Gearman\Job\Data;

class JobStatusData extends JobData {
    public bool $responseReceived = false;
    public bool $isKnown = false;
    public bool $isRunning = false;
}