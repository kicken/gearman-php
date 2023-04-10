<?php

namespace Kicken\Gearman\Server;

use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use function Kicken\Gearman\normalizeFunctionName;

class MemoryJobQueue implements JobQueue, LoggerAwareInterface {
    use LoggerAwareTrait;

    /** @var \SplPriorityQueue[] */
    private array $functionQueues = [];
    private array $handleMap = [];

    public function __construct(?LoggerInterface $logger = null){
        $this->logger = $logger ?? new NullLogger();
    }

    public function enqueue(ServerJobData $jobData) : void{
        $fnKey = normalizeFunctionName($jobData->function);
        if (!isset($this->functionQueues[$fnKey])){
            $this->functionQueues[$fnKey] = new PriorityQueue();
        }

        $this->logger->debug('Inserted job info function queue', [
            'function' => $jobData->function
            , 'priority' => $jobData->priority
        ]);
        $this->functionQueues[$fnKey]->enqueue($jobData);
        $this->handleMap[$fnKey] = $jobData;
    }

    public function dequeue(array $functionList) : ?ServerJobData{
        $queueList = array_map(function(string $fn){
            return $this->functionQueues[normalizeFunctionName($fn)] ?? null;
        }, $functionList);
        $jobToAssign = PriorityQueue::extractHighestAmong($queueList);

        return $jobToAssign;
    }

    public function findByHandle(string $handle) : ?ServerJobData{
        return $this->handleMap[$handle] ?? null;
    }
}
