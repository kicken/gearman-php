<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Events\EventEmitter;
use Kicken\Gearman\Events\ServerEvents;
use Kicken\Gearman\Job\Data\ServerJobData;

class JobQueue {
    use EventEmitter;

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

    public function deleteJob(ServerJobData $jobData){
        if ($jobData->running){
            $this->emit(ServerEvents::JOB_STOPPED, $jobData);
        }

        unset($this->handleMap[$jobData->jobHandle]);
        $this->emit(ServerEvents::JOB_REMOVED, $jobData);
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
