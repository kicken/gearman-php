<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Job\Data\ServerJobData;

class JobQueue {
    private \SplPriorityQueue $queue;
    private array $handleMap = [];
    private WorkerManager $workerRegistry;

    public function __construct(WorkerManager $workerRegistry){
        $this->queue = new \SplPriorityQueue();
        $this->workerRegistry = $workerRegistry;
    }

    public function enqueue(ServerJobData $job){
        $this->queue->insert($job, $job->priority);
        $this->handleMap[$job->jobHandle] = $job;
        $this->workerRegistry->wakeAllCandidates($job);
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

    public function lookupJob(string $handle) : ?ServerJobData{
        return $this->handleMap[$handle] ?? null;
    }

    public function deleteJob(ServerJobData $jobData){
        unset($this->handleMap[$jobData->jobHandle]);
        $nonMatches = [];
        try {
            while ($queuedJob = $this->queue->extract()){
                if ($queuedJob !== $jobData){
                    $nonMatches[] = $queuedJob;
                }
            }
        } catch (\RuntimeException $ex){

        } finally {
            $this->requeue($nonMatches);
        }
    }

    private function requeue(array $jobList) : void{
        /** @var ServerJobData $job */
        foreach ($jobList as $job){
            $this->queue->insert($job, $job->priority);
        }
    }
}
