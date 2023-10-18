<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Events\JobSubmitted;
use Kicken\Gearman\Events\WorkerConnected;
use Kicken\Gearman\Events\WorkerDisconnected;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\ServiceContainer;

class WorkerManager {
    private ServiceContainer $services;
    /** @var \SplObjectStorage */
    private \SplObjectStorage $registry;

    public function __construct(ServiceContainer $container){
        $this->registry = new \SplObjectStorage();
        $this->services = $container;
        $this->services->eventDispatcher->addListener(JobSubmitted::class, function(JobSubmitted $event){
            $this->wakeAllCandidates($event->getJob());
        });
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
            $this->registry->attach($connection, $worker = new Worker($connection, $this->services));
            $this->services->eventDispatcher->dispatch(new WorkerConnected($worker));
        }

        return $this->registry[$connection];
    }

    public function removeConnection(Endpoint $connection){
        $worker = $this->getWorker($connection);
        $this->registry->detach($connection);
        $this->services->eventDispatcher->dispatch(new WorkerDisconnected($worker));
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

    private function wakeAllCandidates(ServerJobData $jobData) : void{
        /** @var Endpoint $connection */
        foreach ($this->registry as $connection){
            $worker = $this->getWorker($connection);
            if ($worker->isSleeping() && $worker->canDo($jobData->function)){
                $worker->wake();
            }
        }
    }
}
