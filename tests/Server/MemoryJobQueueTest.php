<?php

namespace Kicken\Gearman\Test\Server;

use DateTimeImmutable;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Server\JobQueue\MemoryJobQueue;
use Kicken\Gearman\Server\ServerJobData;
use Kicken\Gearman\Server\Worker;
use PHPUnit\Framework\TestCase;

class MemoryJobQueueTest extends TestCase {
    private MemoryJobQueue $testQueue;

    protected function setUp() : void{
        $queue = new MemoryJobQueue();

        $queue->enqueue($this->createJob('H:1', 'test', JobPriority::LOW));
        $queue->enqueue($this->createJob('H:2', 'test'));
        $queue->enqueue($this->createJob('H:3', 'test', JobPriority::HIGH));
        $queue->enqueue($this->createJob('H:4', 'test', JobPriority::LOW));
        $queue->enqueue($this->createJob('H:5', 'test'));
        $queue->enqueue($this->createJob('H:6', 'test', JobPriority::HIGH));

        $this->testQueue = $queue;
    }

    public function testFindsJobsByPriority(){
        $dequeueOrder = $this->dequeueAllJobs();

        $this->assertCount(6, $dequeueOrder);
        $this->assertEquals(['H:3', 'H:6', 'H:2', 'H:5', 'H:1', 'H:4'], $dequeueOrder);
    }

    public function testLookupJobWithValidHandle(){
        $job = $this->testQueue->findByHandle('H:1');
        $this->assertInstanceOf(ServerJobData::class, $job);
    }

    public function testLookupJobWithInvalidHandle(){
        $job = $this->testQueue->findByHandle('InvalidHandle');
        $this->assertNull($job);
    }

    public function testJobCounting(){
        $this->assertEquals(6, $this->testQueue->getTotalJobs('test'));
        $job = $this->testQueue->dequeue(['test']);
        $this->assertEquals(5, $this->testQueue->getTotalJobs('test'));
        $this->testQueue->setRunning($job);
        $this->assertEquals(1, $this->testQueue->getTotalRunning('test'));
        $this->testQueue->setComplete($job);
        $this->assertEquals(0, $this->testQueue->getTotalRunning('test'));
    }

    public function testHasJobForSucceeds(){
        $worker = $this->getMockBuilder(Worker::class)->disableOriginalConstructor()->getMock();
        $worker->method('getAvailableFunctions')->willReturn(['test']);

        $this->assertTrue($this->testQueue->hasJobFor($worker));
    }

    public function testHasJobForFails(){
        $worker = $this->getMockBuilder(Worker::class)->disableOriginalConstructor()->getMock();
        $worker->method('getAvailableFunctions')->willReturn(['invalid']);

        $this->assertFalse($this->testQueue->hasJobFor($worker));
    }

    public function testGetFunctionList(){
        $this->assertEquals(['test'], $this->testQueue->getFunctionList());
        $this->testQueue->enqueue($this->createJob('H:7', 'f1'));
        $this->testQueue->enqueue($this->createJob('H:8', 'f2'));
        $this->testQueue->enqueue($this->createJob('H:9', 'f3'));
        $this->assertEquals(['test', 'f1', 'f2', 'f3'], $this->testQueue->getFunctionList());
    }

    public function testFunctionStaysInList(){
        $this->assertEquals(['test'], $this->testQueue->getFunctionList());
        $this->dequeueAllJobs();
        $this->assertEquals(['test'], $this->testQueue->getFunctionList());
    }

    private function dequeueAllJobs() : array{
        $dequeueOrder = [];
        while ($job = $this->testQueue->dequeue(['test'])){
            $dequeueOrder[] = $job->jobHandle;
        }

        return $dequeueOrder;
    }

    private function createJob(string $handle, string $function, int $priority = JobPriority::NORMAL) : ServerJobData{
        return new ServerJobData($handle, $function, '', 'test work load', $priority, true, new DateTimeImmutable());
    }
}
