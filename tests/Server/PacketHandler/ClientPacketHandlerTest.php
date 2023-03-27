<?php

namespace Kicken\Gearman\Test\Server\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\JobQueue;
use Kicken\Gearman\Server\PacketHandler\ClientPacketHandler;
use Kicken\Gearman\Server\ServerJobData;
use PHPUnit\Framework\TestCase;

class ClientPacketHandlerTest extends TestCase {
    private ClientPacketHandler $handler;
    private Endpoint $connection;
    private JobQueue $queue;

    protected function setUp() : void{
        $this->queue = $this->getMockBuilder(JobQueue::class)->disableOriginalConstructor()->getMock();
        $this->connection = $this->getMockBuilder(Endpoint::class)->getMock();
        $this->handler = new ClientPacketHandler($this->queue);
    }

    public function testSubmitJobPacket(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::SUBMIT_JOB, ['reverse', '', 'test']);
        $this->connection->expects($this->once())->method('writePacket')->with($this->isInstanceOf(BinaryPacket::class));
        $this->queue->expects($this->once())->method('enqueue')->with($this->isInstanceOf(ServerJobData::class));
        $this->handler->handlePacket($this->connection, $packet);
    }

    public function testGetStatusPacket(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::GET_STATUS, ['H:1']);
        $this->queue->expects($this->once())->method('lookupJob')->with('H:1');
        $this->connection->expects($this->once())->method('writePacket')->with($this->isInstanceOf(BinaryPacket::class));
        $this->handler->handlePacket($this->connection, $packet);
    }
}
