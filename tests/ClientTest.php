<?php

namespace Kicken\Gearman\Test;

use Kicken\Gearman\Client;
use Kicken\Gearman\Client\JobStatus;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Test\Network\IncomingPacket;
use Kicken\Gearman\Test\Network\OutgoingPacket;
use Kicken\Gearman\Test\Network\PacketPlaybackConnection;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use React\Promise\PromiseInterface;

class ClientTest extends TestCase {
    public function testSubmitForegroundJobSync(){
        $loop = Loop::get();
        $client = new Client(new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::SUBMIT_JOB, ['reverse', '', 'test'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_COMPLETE, ['H:test:1', 'tset'])
        ]));

        $result = $client->submitJob('reverse', 'test');
        $this->assertEquals('tset', $result);
        $loop->run();
    }

    public function testSubmitForegroundJobAsync(){
        $loop = Loop::get();
        $client = new Client(new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::SUBMIT_JOB, ['reverse', '', 'test'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_COMPLETE, ['H:test:1', 'tset'])
        ]));

        $result = $client->submitJobAsync('reverse', 'test');
        $this->assertInstanceOf(PromiseInterface::class, $result);
        $callables = $this->getMockBuilder(\stdClass::class)->addMethods(['fulfilled', 'rejected'])->getMock();
        $callables->expects($this->once())->method('fulfilled')->with($this->isInstanceOf(Client\ForegroundJob::class));
        $callables->expects($this->never())->method('rejected');
        $result->then([$callables, 'fulfilled'], [$callables, 'rejected']);
        $loop->run();
    }

    public function testSubmitBackgroundJobSync(){
        $loop = Loop::get();
        $client = new Client(new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::SUBMIT_JOB_BG, ['reverse', '', 'test'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
        ]));

        $this->assertEquals('H:test:1', $client->submitBackgroundJob('reverse', 'test'));
        $loop->run();
    }

    public function testSubmitBackgroundJobAsync(){
        $loop = Loop::get();
        $client = new Client(new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::SUBMIT_JOB_BG, ['reverse', '', 'test'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
        ]));

        $result = $client->submitBackgroundJobAsync('reverse', 'test');
        $this->assertInstanceOf(PromiseInterface::class, $result);

        $callables = $this->getMockBuilder(\stdClass::class)->addMethods(['fulfilled', 'rejected'])->getMock();
        $callables->expects($this->once())->method('fulfilled')->with($this->isInstanceOf(Client\BackgroundJob::class));
        $callables->expects($this->never())->method('rejected');
        $result->then([$callables, 'fulfilled'], [$callables, 'rejected']);

        $loop->run();
    }

    public function testGetJobStatusSync(){
        $loop = Loop::get();
        $client = new Client(new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::GET_STATUS, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::STATUS_RES, ['H:test:1', 1, 1, 5, 10])
        ]));

        $status = $client->getJobStatus('H:test:1');
        $this->assertInstanceOf(JobStatus::class, $status);
        $this->assertEquals('H:test:1', $status->getJobHandle());
        $this->assertEquals(1, $status->isKnown());
        $this->assertEquals(1, $status->isRunning());
        $this->assertEquals(5, $status->getNumerator());
        $this->assertEquals(10, $status->getDenominator());
        $loop->run();
    }

    public function testGetJobStatusAsync(){
        $loop = Loop::get();
        $client = new Client(new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::GET_STATUS, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::STATUS_RES, ['H:test:1', 1, 1, 5, 10])
        ]));

        $result = $client->getJobStatusAsync('H:test:1');
        $this->assertInstanceOf(PromiseInterface::class, $result);

        $callables = $this->getMockBuilder(\stdClass::class)->addMethods(['fulfilled', 'rejected'])->getMock();
        $callables->expects($this->once())->method('fulfilled')->with($this->isInstanceOf(Client\JobStatus::class));
        $callables->expects($this->never())->method('rejected');
        $result->then([$callables, 'fulfilled'], [$callables, 'rejected']);

        $loop->run();
    }
}
