<?php

namespace Kicken\Gearman\Test;

use Kicken\Gearman\Client;
use Kicken\Gearman\Job\ClientBackgroundJob;
use Kicken\Gearman\Job\ClientForegroundJob;
use Kicken\Gearman\Job\JobStatus;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Test\Network\AutoPlaybackServer;
use Kicken\Gearman\Test\Network\IncomingPacket;
use Kicken\Gearman\Test\Network\OutgoingPacket;
use Kicken\Gearman\Test\Network\PacketPlaybackServer;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;

class ClientTest extends TestCase {
    public function testDoesConnect(){
        $server = new PacketPlaybackServer();
        $client = new Client($server);
        $client->submitJob('reverse', 'test');

        $this->assertTrue($server->isConnected());
    }

    public function testSubmitForegroundJob(){
        $loop = Loop::get();
        $server = new AutoPlaybackServer([
            new IncomingPacket(PacketMagic::REQ, PacketType::SUBMIT_JOB, ['reverse', '', 'test'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_COMPLETE, ['H:test:1', 'tset'])
        ], $loop);
        $client = new Client($server);

        $mock = $this->getMockBuilder(\stdClass::class)->addMethods(['created', 'completed'])->getMock();
        $mock->expects($this->once())
            ->method('created')
            ->with($this->isInstanceOf(ClientForegroundJob::class))
            ->willReturnCallback(function(ClientForegroundJob $job) use ($mock){
                $this->assertEquals('H:test:1', $job->getJobHandle());
                $job->onComplete([$mock, 'completed']);
            });
        $mock->expects($this->once())
            ->method('completed')
            ->with($this->isInstanceOf(ClientForegroundJob::class))
            ->willReturnCallback(function(ClientForegroundJob $job){
                $this->assertEquals('tset', $job->getResult());
            });

        $client->submitJob('reverse', 'test')->then([$mock, 'created'])->done();
        $loop->run();
    }

    public function testSubmitBackgroundJob(){
        $loop = Loop::get();
        $server = new AutoPlaybackServer([
            new IncomingPacket(PacketMagic::REQ, PacketType::SUBMIT_JOB_BG, ['reverse', '', 'test'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
        ], $loop);
        $client = new Client($server);

        $mock = $this->getMockBuilder(\stdClass::class)->addMethods(['created'])->getMock();
        $mock->expects($this->once())
            ->method('created')
            ->with($this->isInstanceOf(ClientBackgroundJob::class))
            ->willReturnCallback(function(ClientBackgroundJob $job){
                $this->assertEquals('H:test:1', $job->getJobHandle());
            });

        $client->submitBackgroundJob('reverse', 'test')->then([$mock, 'created'])->done();
        $loop->run();

        $this->assertFalse($server->isConnected());
    }

    public function testGetJobStatus(){
        $loop = Loop::get();
        $server = new AutoPlaybackServer([
            new IncomingPacket(PacketMagic::REQ, PacketType::GET_STATUS, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::STATUS_RES, ['H:test:1', 1, 1, 5, 10])
        ], $loop);
        $client = new Client($server);

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

        $this->assertFalse($server->isConnected());
    }
}