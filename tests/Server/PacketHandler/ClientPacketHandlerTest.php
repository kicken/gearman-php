<?php

namespace Kicken\Gearman\Test\Server\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\JobQueue\JobQueue;
use Kicken\Gearman\Server\PacketHandler\ClientPacketHandler;
use Kicken\Gearman\Server\ServerJobData;
use Kicken\Gearman\Server\WorkerManager;
use PHPUnit\Framework\TestCase;

class ClientPacketHandlerTest extends TestCase {
    private ClientPacketHandler $handler;
    private Endpoint $connection;
    private JobQueue $queue;

    protected function setUp() : void{
        $this->queue = $this->getMockBuilder(JobQueue::class)->getMock();
        $this->connection = $this->getMockBuilder(Endpoint::class)->getMock();

        $workManager = new WorkerManager();
        $this->handler = new ClientPacketHandler('H:test', $this->queue, $workManager);
    }

    public function testSubmitJobPacket(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::SUBMIT_JOB, ['reverse', '', 'test']);
        $this->connection->expects($this->once())->method('writePacket')->with($this->isInstanceOf(BinaryPacket::class));
        $this->queue->expects($this->once())->method('enqueue')->with($this->isInstanceOf(ServerJobData::class));
        $this->handler->handlePacket($this->connection, $packet);
    }

    public function testGetStatusPacket(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::GET_STATUS, ['H:1']);
        $this->queue->expects($this->once())->method('findByHandle')->with('H:1');
        $this->connection->expects($this->once())->method('writePacket')->with($this->isInstanceOf(BinaryPacket::class));
        $this->handler->handlePacket($this->connection, $packet);
    }
}
