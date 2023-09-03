<?php

namespace Kicken\Gearman\Test\Server\JobQueue;

use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Server\JobQueue\PriorityQueue;
use Kicken\Gearman\Server\ServerJobData;
use PHPUnit\Framework\TestCase;

class PriorityQueueTest extends TestCase {
    public function testFindHighestPriorityAcrossQueues(){
        $queueNormal = new PriorityQueue();
        $job1 = $this->createJobData('H:test:1', JobPriority::NORMAL);
        $queueNormal->enqueue($job1);
        $queueHigh = new PriorityQueue();
        $job2 = $this->createJobData('H:test:2', JobPriority::HIGH);
        $queueHigh->enqueue($job2);

        $queue = PriorityQueue::findHighestPriorityQueue([$queueNormal, $queueHigh]);
        $this->assertEquals($queue, $queueHigh);
    }

    private function createJobData(string $handle, int $priority) : ServerJobData{
        return new ServerJobData($handle, 'test', '', '', $priority, false, new \DateTimeImmutable());
    }
}
