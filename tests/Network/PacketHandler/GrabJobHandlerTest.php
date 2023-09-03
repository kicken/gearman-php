<?php

namespace Kicken\Gearman\Test\Network\PacketHandler;

use Kicken\Gearman\Exception\NoWorkException;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Test\Network\IncomingPacket;
use Kicken\Gearman\Test\Network\OutgoingPacket;
use Kicken\Gearman\Test\Network\PacketPlaybackConnection;
use Kicken\Gearman\Worker\PacketHandler\GrabJobHandler;
use Kicken\Gearman\Worker\WorkerJob;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class GrabJobHandlerTest extends TestCase {
    public function testGrabJobPacketSent(){
        $server = new PacketPlaybackConnection([
            $packet = new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
        ]);

        $handler = new GrabJobHandler();
        $handler->grabJob($server);
        $server->playback();
        $this->assertTrue($server->didReceivePacket($packet));
    }

    public function testGrabJobResolvesWithJob(){
        $server = new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, ['H:test:1', 'reverse', 'uniq1', 'test'])
        ]);

        $handler = new GrabJobHandler();
        $jobReceived = null;
        $handler->grabJob($server)->then(function(WorkerJob $job) use (&$jobReceived){
            $jobReceived = $job;
        });
        $server->playback();

        $this->assertInstanceOf(WorkerJob::class, $jobReceived);
        $this->assertEquals('reverse', $jobReceived->getFunction());
        $this->assertEquals('uniq1', $jobReceived->getUniqueId());
        $this->assertEquals('test', $jobReceived->getWorkload());
    }

    public function testGrabJobRejectsOnNoJob(){
        $server = new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::NO_JOB)
        ]);
        $handler = new GrabJobHandler();

        $rejectHandler = $this->getMockCallback();
        $rejectHandler->expects($this->once())
            ->method('__invoke')
            ->with($this->isInstanceOf(NoWorkException::class));
        $resolveHandler = $this->getMockCallback();
        $resolveHandler->expects($this->never())
            ->method('__invoke');

        $handler->grabJob($server)->then($resolveHandler, $rejectHandler);
        $server->playback();
    }

    /**
     * @return MockObject|callable
     */
    public function getMockCallback(){
        return $this->getMockBuilder(\stdClass::class)->addMethods(['__invoke'])->getMock();
    }
}
