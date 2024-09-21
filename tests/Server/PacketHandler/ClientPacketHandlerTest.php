<?php

namespace Kicken\Gearman\Test\Server\PacketHandler;

use Kicken\Gearman\Events\JobSubmitted;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\PacketHandler\ClientPacketHandler;
use Kicken\Gearman\Server\WorkerManager;
use Kicken\Gearman\ServiceContainer;
use Kicken\Gearman\Test\MockDispatcher;
use Kicken\Gearman\Test\Network\MockEndpoint;
use PHPUnit\Framework\TestCase;

class ClientPacketHandlerTest extends TestCase {
    private MockEndpoint $connection;
    private MockDispatcher $dispatcher;
    private ClientPacketHandler $handler;

    protected function setUp() : void{
        $this->connection = new MockEndpoint();
        $this->dispatcher = new MockDispatcher();

        $services = new ServiceContainer();
        $workManager = new WorkerManager($services);
        $services->workerManager = $workManager;
        $services->eventDispatcher = $this->dispatcher;

        $this->handler = new ClientPacketHandler($services, 'H:test');
    }

    public function testSubmitJobPacket(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::SUBMIT_JOB, ['reverse', '', 'test']);
        $this->handler->handlePacket($this->connection, $packet);
        $this->assertTrue($this->dispatcher->wasDispatched(JobSubmitted::class));
        $this->assertTrue($this->connection->wasPacketWritten(PacketMagic::RES, PacketType::JOB_CREATED));
    }

    public function testGetStatusPacket(){
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::GET_STATUS, ['H:1']);
        $this->handler->handlePacket($this->connection, $packet);
        $this->assertTrue($this->connection->wasPacketWritten(PacketMagic::RES, PacketType::STATUS_RES));
    }
}
