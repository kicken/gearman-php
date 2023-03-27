<?php

namespace Kicken\Gearman\Test\Server\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\AdministrativeCommandPacket;
use Kicken\Gearman\Protocol\AdministrativePacket;
use Kicken\Gearman\Server;
use Kicken\Gearman\Server\PacketHandler\AdminPacketHandler;
use PHPUnit\Framework\TestCase;

class AdminPacketHandlerTest extends TestCase {
    private Server $server;
    private AdminPacketHandler $handler;
    private Endpoint $connection;

    protected function setUp() : void{
        $this->server = $this->createMock(Server::class);
        $statistics = $this->createMock(Server\Statistics::class);

        $this->handler = new AdminPacketHandler($this->server, $statistics);
        $this->connection = $this->getMockBuilder(Endpoint::class)->getMock();
    }

    public function testHandlesWorkersCommand(){
        $this->runCommandTest(new AdministrativeCommandPacket('workers'));
    }

    public function testHandlesVersionCommand(){
        $this->runCommandTest(new AdministrativeCommandPacket('version'));
    }

    public function testHandlesStatusCommand(){
        $this->runCommandTest(new AdministrativeCommandPacket('status'));
    }

    public function testHandlesShutdownCommand(){
        $packet = new AdministrativeCommandPacket('shutdown');
        $this->server->expects($this->once())->method('shutdown')->with(false);
        $this->handler->handleAdministrativePacket($this->connection, $packet);
    }

    public function testHandlesGracefulShutdownCommand(){
        $packet = new AdministrativeCommandPacket('shutdown graceful');
        $this->server->expects($this->once())->method('shutdown')->with(true);
        $this->handler->handleAdministrativePacket($this->connection, $packet);
    }

    private function runCommandTest(AdministrativeCommandPacket $packet){
        $this->connection->expects($this->once())->method('writePacket')->with($this->isInstanceOf(AdministrativePacket::class));
        $this->handler->handleAdministrativePacket($this->connection, $packet);
    }
}
