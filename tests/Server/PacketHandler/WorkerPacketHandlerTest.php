<?php

namespace Kicken\Gearman\Test\Server\PacketHandler;

use Kicken\Gearman\Events\FunctionRegistered;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server;
use Kicken\Gearman\Server\JobQueue\MemoryJobQueue;
use Kicken\Gearman\Server\PacketHandler\WorkerPacketHandler;
use Kicken\Gearman\Server\ServerJobData;
use Kicken\Gearman\Server\Worker;
use Kicken\Gearman\Server\WorkerManager;
use Kicken\Gearman\ServiceContainer;
use Kicken\Gearman\Test\MockDispatcher;
use Kicken\Gearman\Test\Network\MockEndpoint;
use PHPUnit\Framework\TestCase;

class WorkerPacketHandlerTest extends TestCase {
    private WorkerPacketHandler $handler;
    private MockEndpoint $connection;
    private MockDispatcher $dispatcher;
    private Worker $worker;
    private MemoryJobQueue $jobQueue;

    protected function setUp() : void{
        $server = $this->getMockBuilder(Server::class)->getMock();

        $services = new ServiceContainer();
        $services->workerManager = $manager = new WorkerManager($services);
        $services->eventDispatcher = $this->dispatcher = new MockDispatcher();
        $services->jobQueue = $this->jobQueue = new MemoryJobQueue();

        $this->connection = new MockEndpoint();
        $this->worker = $manager->getWorker($this->connection);
        $this->handler = new WorkerPacketHandler($server, $services);
    }

    public function testCanDo(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::CAN_DO, ['test']);
        $this->handler->handlePacket($this->connection, $packet);
        $this->assertTrue($this->worker->canDo('test'));
        $this->assertTrue($this->dispatcher->wasDispatched(FunctionRegistered::class));
    }

    public function testPreSleep(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::PRE_SLEEP, []);
        $this->handler->handlePacket($this->connection, $packet);
        $this->assertTrue($this->worker->isSleeping());
    }

    public function testGrabJobWhenNoJobAvailable(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::GRAB_JOB, []);
        $this->handler->handlePacket($this->connection, $packet);
        $this->assertTrue($this->connection->wasPacketWritten(PacketMagic::RES, PacketType::NO_JOB));
    }

    public function testGrabJobWithJobAvailable(){
        $this->worker->registerFunction('test');
        $jobData = $this->createJobData();
        $this->jobQueue->enqueue($jobData);

        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::GRAB_JOB, []);
        $this->handler->handlePacket($this->connection, $packet);
        $this->assertTrue($this->connection->wasPacketWritten(PacketMagic::RES, PacketType::JOB_ASSIGN));
        $this->assertTrue($jobData->running);
        $this->assertEquals($jobData, $this->worker->getCurrentJob());
    }

    public function testWorkData(){
        $watcherConnection = $this->addWatcherToJob($this->createJobData());
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::WORK_DATA, ['H:1', 'test']);
        $this->handler->handlePacket($this->connection, $packet);
        $this->assertTrue($watcherConnection->wasPacketWritten(PacketMagic::RES, PacketType::WORK_DATA));
    }

    public function testWorkStatus(){
        $watcherConnection = $this->addWatcherToJob($this->createJobData());
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::WORK_STATUS, ['H:1', '1', '10']);
        $this->handler->handlePacket($this->connection, $packet);
        $this->assertTrue($watcherConnection->wasPacketWritten(PacketMagic::RES, PacketType::WORK_STATUS));
    }

    public function testWorkComplete(){
        $jobData = $this->createJobData();
        $watcherConnection = $this->addWatcherToJob($jobData);
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::WORK_COMPLETE, ['H:1', 'test']);
        $this->handler->handlePacket($this->connection, $packet);
        $this->assertTrue($watcherConnection->wasPacketWritten(PacketMagic::RES, PacketType::WORK_COMPLETE));
        $this->assertNull($this->worker->getCurrentJob());
        $this->assertFalse($jobData->running);
    }

    private function addWatcherToJob(ServerJobData $jobData) : MockEndpoint{
        $watcherConnection = new MockEndpoint();
        $jobData->addWatcher($watcherConnection);

        $this->worker->assignJob($jobData);

        return $watcherConnection;
    }

    private function createJobData() : ServerJobData{
        $jobData = new ServerJobData('H:1', 'test', '', '', JobPriority::NORMAL, false, new \DateTimeImmutable());

        return $jobData;
    }
}
