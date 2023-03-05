<?php

namespace Kicken\Gearman\Test\Server;

use Kicken\Gearman\Job\Data\ServerJobData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Server\JobQueue;
use Kicken\Gearman\Server\Worker;
use Kicken\Gearman\Server\WorkerManager;
use PHPUnit\Framework\TestCase;

class JobQueueTest extends TestCase {
    private JobQueue $testQueue;

    protected function setUp() : void{
        $queue = new JobQueue(new WorkerManager());

        $queue->enqueue(new ServerJobData('H:1', 'test', '', 'low', JobPriority::LOW, true));
        $queue->enqueue(new ServerJobData('H:2', 'test', '', 'normal', JobPriority::NORMAL, true));
        $queue->enqueue(new ServerJobData('H:3', 'test', '', 'high', JobPriority::HIGH, true));
        $queue->enqueue(new ServerJobData('H:4', 'test', '', 'low', JobPriority::LOW, true));
        $queue->enqueue(new ServerJobData('H:5', 'test', '', 'normal', JobPriority::NORMAL, true));
        $queue->enqueue(new ServerJobData('H:6', 'test', '', 'high', JobPriority::HIGH, true));

        $this->testQueue = $queue;
    }

    public function testFindsJobsByPriority(){
        $dequeueOrder = $this->dequeueAllJobs();

        $this->assertCount(6, $dequeueOrder);
        $this->assertEquals(['high', 'high', 'normal', 'normal', 'low', 'low'], $dequeueOrder);
    }

    public function testLookupJobWithValidHandle(){
        $job = $this->testQueue->lookupJob('H:1');
        $this->assertInstanceOf(ServerJobData::class, $job);
    }

    public function testLookupJobWithInvalidHandle(){
        $job = $this->testQueue->lookupJob('InvalidHandle');
        $this->assertNull($job);
    }

    private function dequeueAllJobs() : array{
        $mockWorker = $this->getMockBuilder(Worker::class)->disableOriginalConstructor()->getMock();

        $mockWorker->method('canDo')->with($this->isInstanceOf(ServerJobData::class))->willReturn(true);
        $dequeueOrder = [];
        while ($job = $this->testQueue->assignJob($mockWorker)){
            $dequeueOrder[] = $job->workload;
        }

        return $dequeueOrder;
    }
}
