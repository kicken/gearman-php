<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Events\EventEmitter;
use Kicken\Gearman\Events\ServerEvents;
use Kicken\Gearman\Network\Endpoint;

class WorkerManager {
    use EventEmitter;

    /** @var \SplObjectStorage */
    private \SplObjectStorage $registry;

    public function __construct(){
        $this->registry = new \SplObjectStorage();
    }

    public function wakeAllCandidates(ServerJobData $jobData) : ?Worker{
        /** @var Endpoint $connection */
        foreach ($this->registry as $connection){
            $worker = $this->getWorker($connection);
            if ($worker->isSleeping() && $worker->canDo($jobData->function)){
                $worker->wake();
            }
        }

        return null;
    }

    public function getAllWorkers() : array{
        $workerList = [];
        foreach ($this->registry as $connection){
            $workerList[] = $this->getWorker($connection);
        }

        return $workerList;
    }

    public function getWorker(Endpoint $connection) : Worker{
        if (!$this->registry->contains($connection)){
            $this->registry->attach($connection, $worker = new Worker($connection));
            $this->emit(ServerEvents::WORKER_CONNECTED, $worker);
        }

        return $this->registry[$connection];
    }

    public function removeConnection(Endpoint $connection){
        $worker = $this->getWorker($connection);
        $this->registry->detach($connection);
        $this->emit(ServerEvents::WORKER_DISCONNECTED, $worker);
    }

    public function disconnectAll() : void{
        $this->disconnectByFilter();
    }

    public function disconnectSleeping() : void{
        $this->disconnectByFilter(function(Worker $worker){
            return $worker->isSleeping();
        });
    }

    public function getCapableWorkerCount(string $function) : int{
        $count = 0;
        foreach ($this->registry as $connection){
            $worker = $this->getWorker($connection);
            if ($worker->canDo($function)){
                $count++;
            }

        }

        return $count;
    }

    private function disconnectByFilter(callable $filter = null){
        $connectionList = iterator_to_array($this->registry);
        /** @var Endpoint $connection */
        foreach ($connectionList as $connection){
            if (!$filter || call_user_func($filter, $this->getWorker($connection))){
                $connection->disconnect();
                $this->removeConnection($connection);
            }
        }
    }
}
