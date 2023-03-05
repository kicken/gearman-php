<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Events\EventEmitter;
use Kicken\Gearman\Events\ServerEvents;
use Kicken\Gearman\Job\Data\ServerJobData;
use Kicken\Gearman\Network\Connection;

class WorkerManager {
    use EventEmitter;

    /** @var \SplObjectStorage */
    private \SplObjectStorage $registry;

    public function __construct(){
        $this->registry = new \SplObjectStorage();
    }

    public function wakeAllCandidates(ServerJobData $jobData) : ?Worker{
        /** @var Connection $connection */
        foreach ($this->registry as $connection){
            $worker = $this->getWorker($connection);
            if ($worker->canDo($jobData)){
                $worker->wake();
            }
        }

        return null;
    }

    public function getWorker(Connection $connection) : Worker{
        if (!$this->registry->contains($connection)){
            $this->registry->attach($connection, $worker = new Worker($connection));
            $this->emit(ServerEvents::WORKER_CONNECTED, $worker);
        }

        return $this->registry[$connection];
    }

    public function removeConnection(Connection $connection){
        $worker = $this->getWorker($connection);
        $this->registry->detach($connection);
        $this->emit(ServerEvents::WORKER_DISCONNECTED, $worker);
    }

    public function disconnectAll(){
        $connectionList = iterator_to_array($this->registry);
        /** @var Connection $connection */
        foreach ($connectionList as $connection){
            $connection->disconnect();
            $this->removeConnection($connection);
        }
    }
}
