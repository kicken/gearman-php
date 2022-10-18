<?php

namespace Kicken\Gearman\Test\Server\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Protocol\AdministrativePacket;
use Kicken\Gearman\Server\PacketHandler\AdminPacketHandler;
use Kicken\Gearman\Server\WorkerManager;
use PHPUnit\Framework\TestCase;

class AdminPacketHandlerTest extends TestCase {
    private AdminPacketHandler $handler;
    private Connection $connection;

    protected function setUp() : void{
        $this->handler = new AdminPacketHandler(new WorkerManager());
        $this->connection = $this->getMockBuilder(Connection::class)->getMock();
    }

    public function testHandlesWorkersCommand(){
        $packet = new AdministrativePacket('workers');
        $this->connection->expects($this->once())->method('writePacket')->with($this->isInstanceOf(AdministrativePacket::class));
        $this->handler->handleAdministrativePacket($this->connection, $packet);
    }

    public function testHandlesVersionCommand(){
        $packet = new AdministrativePacket('version');
        $this->connection->expects($this->once())->method('writePacket')->with($this->callback(function($packet){
            return $packet instanceof AdministrativePacket && $packet->getCommand() === 'v0.0.1';
        }));
        $this->handler->handleAdministrativePacket($this->connection, $packet);
    }
}
