<?php

namespace Kicken\Gearman\Test\Server\PacketHandler;

use Kicken\Gearman\Job\Data\ServerJobData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\JobQueue;
use Kicken\Gearman\Server\PacketHandler\WorkerPacketHandler;
use Kicken\Gearman\Server\Worker;
use Kicken\Gearman\Server\WorkerManager;
use PHPUnit\Framework\TestCase;

class WorkerPacketHandlerTest extends TestCase {
    private WorkerPacketHandler $handler;
    private Connection $connection;
    private Worker $worker;
    private JobQueue $queue;
    private WorkerManager $manager;
    private ServerJobData $job;

    protected function setUp() : void{
        $this->connection = $this->getMockBuilder(Connection::class)->getMock();
        $this->manager = $this->getMockBuilder(WorkerManager::class)->disableOriginalConstructor()->getMock();
        $this->worker = $this->getMockBuilder(Worker::class)->setConstructorArgs([$this->connection])->getMock();
        $this->queue = $this->getMockBuilder(JobQueue::class)->setConstructorArgs([$this->manager])->getMock();
        $this->job = $this->getMockBuilder(ServerJobData::class)->setConstructorArgs(['H:1', 'test', '', '', JobPriority::NORMAL, false])->getMock();
        $this->handler = new WorkerPacketHandler($this->manager, $this->queue);
        $this->queue->enqueue($this->job);
    }

    public function testCanDo(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::CAN_DO, ['test']);
        $this->expectGetWorkerCall();
        $this->worker->expects($this->once())->method('registerFunction')->with('test');
        $this->handler->handlePacket($this->connection, $packet);
    }

    public function testPreSleep(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::PRE_SLEEP, []);
        $this->expectGetWorkerCall();
        $this->worker->expects($this->once())->method('sleep');
        $this->handler->handlePacket($this->connection, $packet);
    }

    public function testGrabJob(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::GRAB_JOB, []);
        $this->expectGetWorkerCall();
        $this->queue->expects($this->once())->method('findJob')->with($this->worker)->willReturn($this->job);
        $this->handler->handlePacket($this->connection, $packet);
    }

    public function testWorkData(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::WORK_DATA, ['H:1', 'test']);
        $this->expectGetWorkerCall();
        $this->worker->method('getCurrentJob')->willReturn($this->job);
        $this->handler->handlePacket($this->connection, $packet);
    }

    public function testWorkStatus(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::WORK_STATUS, ['H:1', '1', '10']);
        $this->expectGetWorkerCall();
        $this->expectSendToWatchersCall();
        $this->worker->expects($this->once())->method('getCurrentJob')->willReturn($this->job);
        $this->job->expects($this->once())->method('sendToWatchers')->with($this->isInstanceOf(BinaryPacket::class));
        $this->handler->handlePacket($this->connection, $packet);
    }

    public function testWorkComplete(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::WORK_COMPLETE, ['H:1', 'test']);
        $this->expectGetWorkerCall();
        $this->expectSendToWatchersCall();
        $this->handler->handlePacket($this->connection, $packet);
    }

    private function expectGetWorkerCall(){
        $this->manager->expects($this->once())->method('getWorker')->with($this->connection)->willReturn($this->worker);
    }

    private function expectSendToWatchersCall(){
        $this->worker->expects($this->once())->method('getCurrentJob')->willReturn($this->job);
        $this->job->expects($this->once())->method('sendToWatchers')->with($this->isInstanceOf(BinaryPacket::class));
    }
}
