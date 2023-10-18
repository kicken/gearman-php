<?php

namespace Kicken\Gearman\Events;

use Kicken\Gearman\Server\ServerJobData;

class JobStarted {
    public ServerJobData $job;

    public function __construct(ServerJobData $job){
        $this->job = $job;
    }
}
