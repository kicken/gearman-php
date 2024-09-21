<?php

namespace Kicken\Gearman\Test\Server;

use Kicken\Gearman\Events\FunctionRegistered;
use Kicken\Gearman\Events\FunctionUnregistered;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\Worker;
use Kicken\Gearman\ServiceContainer;
use Kicken\Gearman\Test\MockDispatcher;
use Kicken\Gearman\Test\Network\MockEndpoint;
use PHPUnit\Framework\TestCase;

class WorkerTest extends TestCase {
    private Worker $worker;
    private Endpoint $connection;
    private MockDispatcher $dispatcher;

    protected function setUp() : void{
        $this->connection = new MockEndpoint();
        $serviceContainer = new ServiceContainer();
        $serviceContainer->eventDispatcher = $this->dispatcher = new MockDispatcher();

        $this->worker = new Worker($this->connection, $serviceContainer);
    }

    public function testWake(){
        //Ensure worker is sleeping.
        $this->worker->sleep();

        $this->worker->wake();
        $this->assertTrue($this->connection->wasPacketWritten(PacketMagic::RES, PacketType::NOOP));
    }

    public function testRegisterFunctionTriggersEvent(){
        $this->worker->registerFunction('test');
        $this->assertTrue($this->worker->canDo('test'));
        $this->assertTrue($this->dispatcher->wasDispatched(FunctionRegistered::class));
    }

    public function testCanDo(){
        $this->worker->registerFunction('test');
        $this->assertTrue($this->worker->canDo('test'));
        $this->assertTrue($this->worker->canDo('Test'));
    }

    public function testUnregisterFunctionTriggersEvent(){
        $this->worker->registerFunction('test');
        $this->worker->unregisterFunction('test');
        $this->assertFalse($this->worker->canDo('test'));
        $this->assertTrue($this->dispatcher->wasDispatched(FunctionUnregistered::class));
    }

    public function testGetAvailableFunctions(){
        $this->assertEquals([], $this->worker->getAvailableFunctions());
        $this->worker->registerFunction('test');
        $this->assertEquals(['test'], $this->worker->getAvailableFunctions());
        $this->worker->unregisterFunction('test');
        $this->assertEquals([], $this->worker->getAvailableFunctions());
    }
}
