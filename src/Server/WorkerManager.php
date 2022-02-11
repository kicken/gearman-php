<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Job\Data\ServerJobData;
use Kicken\Gearman\Network\Connection;

class WorkerManager {
    /** @var \SplObjectStorage */
    private \SplObjectStorage $registry;

    public function __construct(){
        $this->registry = new \SplObjectStorage();
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

    public function findWorker(ServerJobData $jobData) : ?Worker{
        /** @var Connection $connection */
        foreach ($this->registry as $connection){
            $worker = $this->getWorker($connection);
            if ($worker->canDo($jobData)){
                return $worker;
            }
        }

        return null;
    }

    public function getWorker(Connection $connection) : Worker{
        if (!$this->registry->contains($connection)){
            $this->registry->attach($connection, new Worker($connection));
        }

        return $this->registry[$connection];
    }
}
