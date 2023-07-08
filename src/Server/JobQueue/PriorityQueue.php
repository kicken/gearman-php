<?php

namespace Kicken\Gearman\Server\JobQueue;

use Kicken\Gearman\Server\ServerJobData;

class PriorityQueue extends \SplPriorityQueue {
    /**
     * @param PriorityQueue[] $queueList
     *
     * @return ?PriorityQueue
     */
    public static function findHighestPriorityQueue(array $queueList) : ?PriorityQueue{
        $priorityList = [];
        foreach ($queueList as $queue){
            if ($queue && !$queue->isEmpty()){
                $priorityList[] = [$queue->top(), $queue];
            }
        }
        if (!$priorityList){
            return null;
        }

        usort($priorityList, function(array $a, array $b){
            return self::compareJobs($a[0], $b[0]);
        });

        /** @var PriorityQueue $queue */
        $queue = $priorityList[0][1];

        return $queue;
    }

    private static function compareJobs(ServerJobData $a, ServerJobData $b) : int{
        $result = $a->priority <=> $b->priority;
        if ($result === 0){
            $result = ($a->created <=> $b->created) * -1;
        }

        return $result;
    }

    public function compare($priority1, $priority2) : int{
        if (!$priority1 instanceof ServerJobData || !$priority2 instanceof ServerJobData){
            throw new \InvalidArgumentException();
        }

        return self::compareJobs($priority1, $priority2);
    }

    public function enqueue(ServerJobData $jobData) : bool{
        return $this->insert($jobData, $jobData);
    }

    public function insert($value, $priority) : bool{
        if (!$value instanceof ServerJobData || !$priority instanceof ServerJobData){
            throw new \InvalidArgumentException();
        }

        return parent::insert($value, $priority);
    }
}
