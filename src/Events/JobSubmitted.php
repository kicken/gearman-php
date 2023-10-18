<?php

namespace Kicken\Gearman\Events;

use Kicken\Gearman\Server\ServerJobData;
use Symfony\Contracts\EventDispatcher\Event;

class JobSubmitted extends Event {
    private ServerJobData $job;

    public function __construct(ServerJobData $job){
        $this->job = $job;
    }

    public function getJob() : ServerJobData{
        return $this->job;
    }
}
