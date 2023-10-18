<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Events\WorkerConnected;
use Kicken\Gearman\Events\WorkerDisconnected;
use Kicken\Gearman\ServiceContainer;

class Statistics {
    private ServiceContainer $services;
    private array $workerList = [];
    private array $functionQueueStats = [];

    public function __construct(ServiceContainer $container){
        $this->services = $container;
        $dispatcher = $container->eventDispatcher;
        $dispatcher->addListener(WorkerConnected::class, function(WorkerConnected $event){
            $this->workerList[] = \WeakReference::create($event->worker);
        });
        $dispatcher->addListener(WorkerDisconnected::class, function(WorkerDisconnected $event){
            $key = array_search($event->worker, $this->workerList, true);
            unset($this->workerList[$key]);
        });
    }

    public function listWorkerDetails() : string{
        $list = [];
        foreach ($this->services->workerManager->getAllWorkers() as $worker){
            $list[] = sprintf('%d %s %s: %s'
                , $worker->getConnection()->getFd()
                , $worker->getConnection()->getAddress()
                , $worker->getConnection()->getClientId()
                , implode(' ', $worker->getAvailableFunctions())
            );
        }

        return $this->implodeList($list);
    }

    public function listQueueDetails() : string{
        $list = [];
        foreach ($this->services->jobQueue->getFunctionList() as $function){
            $list[] = sprintf("%s\t%d\t%d\t%d",
                $function,
                $this->services->jobQueue->getTotalJobs($function),
                $this->services->jobQueue->getTotalRunning($function),
                $this->services->workerManager->getCapableWorkerCount($function)
            );
        }

        return $this->implodeList($list);
    }

    private function implodeList(array $list) : string{
        $response = implode("\r\n", $list);
        if ($response !== ''){
            $response .= "\r\n";
        }

        return $response . '.';
    }
}
