<?php

namespace Kicken\Gearman\Test\Network\PacketHandler;

use Kicken\Gearman\Job\WorkerJob;
use Kicken\Gearman\Network\PacketHandler\GrabJobHandler;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Test\Network\IncomingPacket;
use Kicken\Gearman\Test\Network\OutgoingPacket;
use Kicken\Gearman\Test\Network\PacketPlaybackConnection;
use PHPUnit\Framework\TestCase;

class GrabJobHandlerTest extends TestCase {
    public function testGrabJobPacketSent(){
        $server = new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
        ]);

        $handler = new GrabJobHandler();
        $server->connect()->then(function(PacketPlaybackConnection $server) use ($handler){
            $handler->grabJob($server);
            $server->playback();
        });

        $this->assertTrue($server->hasHandler($handler));
    }

    public function testGrabJobSuccessful(){
        $server = new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, ['H:test:1', 'reverse', 'uniq1', 'test'])
        ]);

        $handler = new GrabJobHandler();
        $jobReceived = null;
        $server->connect()->then(function(PacketPlaybackConnection $server) use (&$jobReceived, $handler){
            $handler->grabJob($server)->then(function(WorkerJob $job) use (&$jobReceived){
                $jobReceived = $job;
            });
            $server->playback();
        });

        $this->assertInstanceOf(WorkerJob::class, $jobReceived);
        $this->assertEquals('reverse', $jobReceived->getFunction());
        $this->assertEquals('uniq1', $jobReceived->getUniqueId());
        $this->assertEquals('test', $jobReceived->getWorkload());
        $this->assertFalse($server->hasHandler($handler));
    }

    public function testGrabJobSleepsOnNoJob(){
        $server = new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::NO_JOB)
            , new IncomingPacket(PacketMagic::REQ, PacketType::PRE_SLEEP)
        ]);
        $handler = new GrabJobHandler();
        $server->connect()->then(function(PacketPlaybackConnection $server) use ($handler){
            $handler->grabJob($server);
            $server->playback();
        });

        $this->expectNotToPerformAssertions();
    }

    public function testGrabJobSuccessfulAfterSleep(){
        $server = new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::NO_JOB)
            , new IncomingPacket(PacketMagic::REQ, PacketType::PRE_SLEEP)
            , new OutgoingPacket(PacketMagic::RES, PacketType::NOOP)
            , new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, ['H:test:1', 'reverse', 'uniq1', 'test'])
        ]);
        $handler = new GrabJobHandler();
        $jobReceived = null;

        $server->connect()->then(function(PacketPlaybackConnection $server) use ($handler, &$jobReceived){
            $handler->grabJob($server)->then(function(WorkerJob $job) use (&$jobReceived){
                $jobReceived = $job;
            });
            $server->playback();
        });

        $this->assertInstanceOf(WorkerJob::class, $jobReceived);
        $this->assertEquals('reverse', $jobReceived->getFunction());
        $this->assertEquals('uniq1', $jobReceived->getUniqueId());
        $this->assertEquals('test', $jobReceived->getWorkload());
        $this->assertFalse($server->hasHandler($handler));
    }
}
