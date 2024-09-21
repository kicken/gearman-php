<?php

namespace Kicken\Gearman\Test\Server;

use Kicken\Gearman\Events\JobSubmitted;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Server\ServerJobData;
use Kicken\Gearman\Server\Worker;
use Kicken\Gearman\Server\WorkerManager;
use Kicken\Gearman\ServiceContainer;
use Kicken\Gearman\Test\MockDispatcher;
use Kicken\Gearman\Test\Network\MockEndpoint;
use PHPUnit\Framework\TestCase;

class WorkerManagerTest extends TestCase {
    private WorkerManager $manager;
    private MockDispatcher $dispatcher;

    public function setUp() : void{
        $services = new ServiceContainer();
        $services->eventDispatcher = $this->dispatcher = new MockDispatcher();
        $this->manager = new WorkerManager($services);
    }

    public function testGetWorkerAddsWorker(){
        $this->assertCount(0, $this->manager->getAllWorkers());
        $this->addWorkers(1);
        $this->assertCount(1, $this->manager->getAllWorkers());
    }

    public function testWakesAllCandidatesWhenJobSubmitted(){
        $jobData = $this->createTestJob();
        $workers = $this->addWorkers(4);
        foreach ($workers as $worker){
            $this->assertTrue($worker->isSleeping());
        }

        $this->dispatcher->dispatch(new JobSubmitted($jobData));
        foreach ($workers as $worker){
            $this->assertFalse($worker->isSleeping());
        }
    }

    public function testWakesOnlyEligibleCandidates(){
        $jobData = $this->createTestJob();
        $eligibleWorkers = $this->addWorkers(4);
        $ineligibleWorkers = $this->addWorkers(4, ['other-test']);
        foreach ([...$eligibleWorkers, ...$ineligibleWorkers] as $worker){
            $this->assertTrue($worker->isSleeping());
        }

        $this->dispatcher->dispatch(new JobSubmitted($jobData));
        foreach ($ineligibleWorkers as $worker){
            $this->assertTrue($worker->isSleeping());
        }
        foreach ($eligibleWorkers as $worker){
            $this->assertFalse($worker->isSleeping());
        }
    }

    public function testRemoveConnectionRemovesWorker(){
        $worker = $this->createTestWorker();
        $this->assertCount(1, $this->manager->getAllWorkers());
        $this->manager->removeConnection($worker->getConnection());
        $this->assertCount(0, $this->manager->getAllWorkers());
    }


    public function testDisconnectAll(){
        $this->addWorkers(4);
        $this->assertCount(4, $this->manager->getAllWorkers());
        $this->manager->disconnectAll();
        $this->assertCount(0, $this->manager->getAllWorkers());
    }

    public function testDisconnectSleeping(){
        $awakeWorkers = $this->addWorkers(2);
        foreach ($awakeWorkers as $worker){
            $worker->wake();
        }

        $this->addWorkers(2, ['other-test']);
        $this->assertCount(4, $this->manager->getAllWorkers());
        $this->manager->disconnectSleeping();
        $this->assertCount(2, $this->manager->getAllWorkers());
    }

    public function testCapableWorkerCount(){
        $this->addWorkers(2);
        $this->addWorkers(2, ['other-test']);
        $this->assertCount(4, $this->manager->getAllWorkers());
        $this->assertEquals(2, $this->manager->getCapableWorkerCount('test'));
    }

    private function createTestJob() : ServerJobData{
        return new ServerJobData(
            'H:test:1',
            'test',
            '1234',
            'test',
            JobPriority::NORMAL,
            false,
            new \DateTimeImmutable()
        );
    }

    private function createTestWorker(array $functionList = ['test']) : Worker{
        $worker = $this->manager->getWorker(new MockEndpoint());
        foreach ($functionList as $fn){
            $worker->registerFunction($fn);
        }
        $worker->sleep();

        return $worker;
    }

    private function addWorkers(int $number, array $functionList = ['test']) : array{
        /** @var Worker[] $workers */
        $workers = [];
        for ($i = 0; $i < $number; $i++){
            $workers[$i] = $this->createTestWorker($functionList);
            $this->assertTrue($workers[$i]->isSleeping());
        }

        return $workers;
    }
}
