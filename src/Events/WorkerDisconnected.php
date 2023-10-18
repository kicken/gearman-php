<?php

namespace Kicken\Gearman\Events;

use Kicken\Gearman\Server\Worker;

class WorkerDisconnected {
    public Worker $worker;

    public function __construct(Worker $worker){
        $this->worker = $worker;
    }
}
