<?php

namespace Kicken\Gearman\Test\Server;

use Kicken\Gearman\Job\Data\ServerJobData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Server\Worker;
use PHPUnit\Framework\TestCase;

class WorkerTest extends TestCase {
    private Worker $worker;
    private Connection $connection;

    protected function setUp() : void{
        $this->connection = $this->getMockBuilder(Connection::class)->getMock();
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
        $job = new ServerJobData('H:1', 'test', '', '', JobPriority::NORMAL, false);
        $this->assertTrue($this->worker->canDo($job));
    }

    public function testUnregisterFunction(){
        $job = new ServerJobData('H:1', 'test', '', '', JobPriority::NORMAL, false);
        $this->worker->unregisterFunction('test');
        $this->assertFalse($this->worker->canDo($job));
    }

    public function testGetAvailableFunctions(){
        $this->assertEquals(['test'], $this->worker->getAvailableFunctions());
    }
}
