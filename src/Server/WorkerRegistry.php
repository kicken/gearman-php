<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Network\Connection;

class WorkerRegistry {
    private \SplObjectStorage $registry;

    public function __construct(){
        $this->registry = new \SplObjectStorage();
    }

    public function registerWorker(Connection $connection, string $function, ?int $timeout){
        $worker = $this->getWorker($connection);
        $worker->registerFunction($function, $timeout);
    }

    public function listWorkerDetails() : string{
        $list = [];
        /** @var Connection $connection */
        foreach ($this->registry as $connection){
            $worker = $this->getWorker($connection);
            $list[] = sprintf('%d %s %s: %s'
                , $connection->getFd()
                , $connection->getRemoteAddress()
                , ''
                , implode(' ', $worker->getAvailableFunctions())
            );
        }

        return implode("\n", $list) . "\n.";
    }

    private function getWorker(Connection $connection) : Worker{
        if (!$this->registry->contains($connection)){
            $this->registry->attach($connection, new Worker());
        }

        return $this->registry[$connection];
    }
}
