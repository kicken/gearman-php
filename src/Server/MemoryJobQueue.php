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
            $this->functionQueues[$fnKey] = new \SplPriorityQueue();
        }

        $this->logger->debug('Inserted job info function queue', [
            'function' => $jobData->function
            , 'priority' => $jobData->priority
        ]);
        $this->functionQueues[$fnKey]->insert($jobData, $jobData->priority);
        $this->handleMap[$fnKey] = $jobData;
    }

    public function dequeue(array $functionList) : ?ServerJobData{
        $priorityList = [];
        foreach ($functionList as $fn){
            $fnKey = normalizeFunctionName($fn);
            $queue = $this->functionQueues[$fnKey] ?? null;
            if (!$queue || $queue->isEmpty()){
                continue;
            }

            $queue->setExtractFlags(\SplPriorityQueue::EXTR_PRIORITY);
            $priority = $queue->top();
            $queue->setExtractFlags(\SplPriorityQueue::EXTR_DATA);
            if ($priority !== false){
                $this->logger->debug('Found possible job for function', [
                    'function' => $fn
                    , 'priority' => $priority
                ]);
                $priorityList[] = [$priority, $fnKey];
            }
        }

        if (!$priorityList){
            return null;
        }

        array_multisort(array_column($priorityList, 0), SORT_ASC, SORT_NUMERIC, $priorityList);
        $highFunction = $priorityList[0][1];
        /** @var ServerJobData $jobToAssign */
        $jobToAssign = $this->functionQueues[$highFunction]->extract();
        $this->logger->debug('Dequeued job', [
            'function' => $jobToAssign->function
            , 'handle' => $jobToAssign->jobHandle
            , 'priority' => $jobToAssign->priority
        ]);

        return $jobToAssign;
    }

    public function findByHandle(string $handle) : ?ServerJobData{
        return $this->handleMap[$handle] ?? null;
    }
}
