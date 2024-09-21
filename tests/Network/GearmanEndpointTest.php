<?php

namespace Kicken\Gearman\Test\Network;

use Kicken\Gearman\Events\ClientConnected;
use Kicken\Gearman\Events\ClientDisconnected;
use Kicken\Gearman\Exception\CouldNotConnectException;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\GearmanEndpoint;
use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\ServiceContainer;
use Kicken\Gearman\Test\MockDispatcher;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class GearmanEndpointTest extends TestCase {
    private LoopInterface $loop;
    private MockServer $mockServer;
    private MockDispatcher $mockDispatcher;
    private GearmanEndpoint $endpoint;

    protected function setUp() : void{
        $this->loop = Loop::get();
        $this->mockServer = new MockServer($this->loop);
        $this->mockDispatcher = new MockDispatcher();
        $services = new ServiceContainer();
        $services->loop = $this->loop;
        $services->eventDispatcher = $this->mockDispatcher;
        $this->endpoint = new GearmanEndpoint($this->mockServer->listen(), null, $services);
    }

    protected function tearDown() : void{
        $this->mockServer->shutdown();
    }

    public function testConnectSuccess(){
        $success = false;
        $this->endpoint->connect()->then(function($endpoint) use (&$success){
            $this->assertInstanceOf(Endpoint::class, $endpoint);
            $endpoint->disconnect();
            $success = true;
        }, function(){
            $this->fail('Connect rejected handler should not be called.');
        });

        $this->loop->run();
        $this->assertTrue($success);
    }

    public function testConnectFailure(){
        $success = false;
        $this->mockServer->shutdown();
        $this->endpoint->connect(true)->then(function(){
            $this->fail('Connect fulfilled handler should not be called.');
        }, function($error) use (&$success){
            $this->assertInstanceOf(CouldNotConnectException::class, $error);
            $success = true;
        });
        $this->loop->run();
        $this->assertTrue($success);
    }

    public function testConnectDisconnectEventEmitted(){
        $this->endpoint->connect()->then(function(){
            $this->endpoint->disconnect();
            $this->mockServer->shutdown();
        });
        $this->loop->run();
        $this->assertTrue($this->mockDispatcher->wasDispatched(ClientConnected::class));
        $this->assertTrue($this->mockDispatcher->wasDispatched(ClientDisconnected::class));
    }

    public function testWritePacket(){
        $this->endpoint->connect()->then(function(Endpoint $endpoint){
            $packet = new BinaryPacket(PacketMagic::REQ, PacketType::ECHO_REQ, [time()]);
            $endpoint->writePacket($packet);
            $this->loop->addTimer(0.1, function() use ($packet){
                $this->assertTrue($this->mockServer->receivedPacket($packet));
                $this->endpoint->disconnect();
                $this->mockServer->shutdown();
            });
        });
        $this->loop->run();
    }

    public function testEmitReceivedPacket(){
        $this->endpoint->connect()->then(function(){
            $packet = new BinaryPacket(PacketMagic::REQ, PacketType::ECHO_REQ, [time()]);

            $handler = $this->createMock(PacketHandler::class);
            $handler->expects($this->atLeast(1))->method('handlePacket')->with(
                $this->endpoint
                , $packet
            );
            $this->mockServer->writePacket($packet);
            $this->endpoint->addPacketHandler($handler);
            $this->loop->addTimer(0.1, function() use ($packet){
                $this->endpoint->disconnect();
                $this->mockServer->shutdown();
            });
        });
        $this->loop->run();
    }
}
