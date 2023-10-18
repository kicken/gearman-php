<?php

namespace Kicken\Gearman\Events;

use Kicken\Gearman\Server\Worker;
use Symfony\Contracts\EventDispatcher\Event;

class WorkerConnected extends Event {
    public Worker $worker;

    public function __construct(Worker $worker){
        $this->worker = $worker;
    }
}
