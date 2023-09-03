<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Server\JobQueue\JobQueue;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;

class Statistics implements LoggerAwareInterface {
    use LoggerAwareTrait;

    private JobQueue $jobQueue;
    private WorkerManager $workerManager;

    private array $workerList = [];
    private array $functionQueueStats = [];

    public function __construct(WorkerManager $registry, JobQueue $jobQueue, LoggerInterface $logger){
        $this->logger = $logger;
        $this->jobQueue = $jobQueue;
        $this->workerManager = $registry;
    }

    public function listWorkerDetails() : string{
        $list = [];
        foreach ($this->workerManager->getAllWorkers() as $worker){
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
        foreach ($this->jobQueue->getFunctionList() as $function){
            $list[] = sprintf("%s\t%d\t%d\t%d",
                $function,
                $this->jobQueue->getTotalJobs($function),
                $this->jobQueue->getTotalRunning($function),
                $this->workerManager->getCapableWorkerCount($function)
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
