<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Job\Data\ServerJobData;

class JobQueue {
    private \SplPriorityQueue $queue;
    private WorkerManager $workerRegistry;

    public function __construct(WorkerManager $workerRegistry){
        $this->queue = new \SplPriorityQueue();
        $this->workerRegistry = $workerRegistry;
    }

    public function enqueue(ServerJobData $job){
        $this->queue->insert($job, $job->priority);
        $worker = $this->workerRegistry->findWorker($job);
        if ($worker){
            $worker->wake();
        }
    }

    public function findJob(Worker $worker) : ?ServerJobData{
        $unassigned = [];
        try {
            while ($job = $this->queue->extract()){
                if ($worker->canDo($job)){
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

    private function requeue(array $jobList) : void{
        /** @var ServerJobData $job */
        foreach ($jobList as $job){
            $this->queue->insert($job, $job->priority);
        }
    }
}
