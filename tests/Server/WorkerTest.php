<?php

namespace Kicken\Gearman\Test\Server;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Server\Worker;
use PHPUnit\Framework\TestCase;

class WorkerTest extends TestCase {
    private Worker $worker;
    private Endpoint $connection;

    protected function setUp() : void{
        $this->connection = $this->getMockBuilder(Endpoint::class)->getMock();
        $this->worker = new Worker($this->connection);
        $this->worker->registerFunction('test');
    }

    public function testWake(){
        //Ensure worker is sleeping.
        $this->worker->sleep();

        $this->connection->expects($this->once())->method('writePacket')->with($this->isInstanceOf(BinaryPacket::class));
        $this->worker->wake();
    }

    public function testCanDo(){
        $this->assertTrue($this->worker->canDo('test'));
    }

    public function testUnregisterFunction(){
        $this->worker->unregisterFunction('test');
        $this->assertFalse($this->worker->canDo('test'));
    }

    public function testGetAvailableFunctions(){
        $this->assertEquals(['test'], $this->worker->getAvailableFunctions());
    }
}
