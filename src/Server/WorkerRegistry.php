<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Network\Connection;

class WorkerRegistry {
    private \SplObjectStorage $registry;

    public function __construct(){
        $this->registry = new \SplObjectStorage();
    }

    public function getWorker(Connection $connection) : ?WorkerConnection{
        if ($this->registry->contains($connection)){
            return $this->registry[$connection];
        }

        $worker = new WorkerConnection($connection);
        $this->registry[$connection] = $worker;

        return $worker;
    }

    public function createWorker(Connection $connection) : WorkerConnection{
        return $this->registry[$connection] = new WorkerConnection($connection);
    }
}
