<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Events\EventEmitter;
use Kicken\Gearman\Events\ServerEvents;

class JobQueue {
    use EventEmitter;

    private \SplPriorityQueue $queue;
    private array $handleMap = [];
    private WorkerManager $workerRegistry;
    private string $handlePrefix;

    public function __construct(WorkerManager $workerRegistry){
        $this->queue = new \SplPriorityQueue();
        $this->handlePrefix = 'H:' . bin2hex(random_bytes(4));
        $this->workerRegistry = $workerRegistry;
        $this->workerRegistry->on(ServerEvents::WORKER_CONNECTED, function(Worker $worker){
            $worker->on(ServerEvents::JOB_STOPPED, function(ServerJobData $job){
                unset($this->handleMap[$job->jobHandle]);
            });
        });
    }

    public function setHandlePrefix(string $prefix){
        $this->handlePrefix = $prefix;
    }

    public function enqueue(ServerJobData $job){
        if (!$job->jobHandle){
            $job->jobHandle = $this->newHandle();
        }
        $this->queue->insert($job, $job->priority);
        $this->handleMap[$job->jobHandle] = $job;
        $this->workerRegistry->wakeAllCandidates($job);
        $this->emit(ServerEvents::JOB_QUEUED, $job);
    }

    public function assignJob(Worker $worker) : ?ServerJobData{
        $unassigned = [];
        try {
            while ($job = $this->queue->extract()){
                if ($worker->canDo($job)){
                    $worker->assignJob($job);
                    $this->emit(ServerEvents::JOB_STARTED, $job);

                    return $job;
                }

                $unassigned[] = $job;
            }

        } catch (\RuntimeException $ex){
        } finally {
            $this->requeue($unassigned);
        }

        return null;
    }

    public function lookupJob(string $handle) : ?ServerJobData{
        return $this->handleMap[$handle] ?? null;
    }

    private function requeue(array $jobList) : void{
        /** @var ServerJobData $job */
        foreach ($jobList as $job){
            $this->queue->insert($job, $job->priority);
        }
    }

    private function newHandle() : string{
        static $counter = 0;

        return $this->handlePrefix . ':' . (++$counter);
    }
}
