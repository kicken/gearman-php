<?php

namespace Kicken\Gearman\Test;

use Kicken\Gearman\Client;
use Kicken\Gearman\Client\BackgroundJob;
use Kicken\Gearman\Client\ForegroundJob;
use Kicken\Gearman\Client\JobStatus;
use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\GearmanEndpoint;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Test\Network\AutoPlaybackServer;
use Kicken\Gearman\Test\Network\IncomingPacket;
use Kicken\Gearman\Test\Network\OutgoingPacket;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use function React\Promise\resolve;

class ClientTest extends TestCase {
    public function testDoesConnect(){
        $connection = $this->getMockBuilder(Connection::class)->getMock();
        $client = $this->createClient($connection);
        $client->submitJob('reverse', 'test');
    }

    public function testSubmitForegroundJob(){
        $loop = Loop::get();
        $client = $this->createClient(new AutoPlaybackServer([
            new IncomingPacket(PacketMagic::REQ, PacketType::SUBMIT_JOB, ['reverse', '', 'test'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_COMPLETE, ['H:test:1', 'tset'])
        ], $loop));

        $mock = $this->getMockBuilder(\stdClass::class)->addMethods(['created', 'completed'])->getMock();
        $mock->expects($this->once())
            ->method('created')
            ->with($this->isInstanceOf(ForegroundJob::class))
            ->willReturnCallback(function(ForegroundJob $job) use ($mock){
                $this->assertEquals('H:test:1', $job->getJobHandle());
                $job->onComplete([$mock, 'completed']);
            });
        $mock->expects($this->once())
            ->method('completed')
            ->with($this->isInstanceOf(ForegroundJob::class))
            ->willReturnCallback(function(ForegroundJob $job){
                $this->assertEquals('tset', $job->getResult());
            });

        $client->submitJob('reverse', 'test')->then([$mock, 'created'])->done();
        $loop->run();
    }

    public function testSubmitBackgroundJob(){
        $loop = Loop::get();
        $client = $this->createClient(new AutoPlaybackServer([
            new IncomingPacket(PacketMagic::REQ, PacketType::SUBMIT_JOB_BG, ['reverse', '', 'test'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
        ], $loop));

        $mock = $this->getMockBuilder(\stdClass::class)->addMethods(['created'])->getMock();
        $mock->expects($this->once())
            ->method('created')
            ->with($this->isInstanceOf(BackgroundJob::class))
            ->willReturnCallback(function(BackgroundJob $job){
                $this->assertEquals('H:test:1', $job->getJobHandle());
            });

        $client->submitBackgroundJob('reverse', 'test')->then([$mock, 'created'])->done();
        $loop->run();
    }

    public function testGetJobStatus(){
        $loop = Loop::get();
        $client = $this->createClient(new AutoPlaybackServer([
            new IncomingPacket(PacketMagic::REQ, PacketType::GET_STATUS, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::STATUS_RES, ['H:test:1', 1, 1, 5, 10])
        ], $loop));

        $mock = $this->getMockBuilder(\stdClass::class)->addMethods(['statusReady'])->getMock();
        $mock->expects($this->once())
            ->method('statusReady')
            ->with($this->isInstanceOf(JobStatus::class))
            ->willReturnCallback(function(JobStatus $job){
                $this->assertEquals('H:test:1', $job->getJobHandle());
                $this->assertEquals(5, $job->getNumerator());
                $this->assertEquals(10, $job->getDenominator());
                $this->assertTrue($job->isKnown());
                $this->assertTrue($job->isRunning());
            });

        $client->getJobStatus('H:test:1')->then([$mock, 'statusReady'])->done();
        $loop->run();
    }

    private function createClient(Connection $connection) : Client{
        $endpoint = $this->getMockBuilder(GearmanEndpoint::class)->disableOriginalConstructor()->getMock();
        $endpoint->expects($this->atLeastOnce())->method('connect')->willReturn(resolve($connection));

        return new Client($endpoint);
    }
}
