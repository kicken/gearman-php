<?php

namespace Kicken\Gearman\Test\Network\PacketHandler;

use Kicken\Gearman\Job\Data\JobStatusData;
use Kicken\Gearman\Job\JobStatus;
use Kicken\Gearman\Network\PacketHandler\JobStatusHandler;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Test\Network\IncomingPacket;
use Kicken\Gearman\Test\Network\OutgoingPacket;
use Kicken\Gearman\Test\Network\PacketPlaybackServer;
use PHPUnit\Framework\TestCase;

class JobStatusHandlerTest extends TestCase {
    public function testStatusRequestSent(){
        $server = new PacketPlaybackServer([
            new IncomingPacket(PacketMagic::REQ, PacketType::GET_STATUS, ['H:test:1'])
        ]);

        $data = new JobStatusData('H:test:1');
        $handler = new JobStatusHandler($data);
        $server->connect()->then(function(PacketPlaybackServer $server) use ($handler){
            $handler->waitForResult($server);
            $server->playback();
        });

        $this->assertTrue($server->hasHandler($handler));
    }

    public function testStatusReceived(){
        $server = new PacketPlaybackServer([
            new IncomingPacket(PacketMagic::REQ, PacketType::GET_STATUS, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::STATUS_RES, ['H:test:1', 1, 1, 5, 10])
        ]);

        $data = new JobStatusData('H:test:1');
        $handler = new JobStatusHandler($data);

        $statusReceived = null;
        $server->connect()->then(function(PacketPlaybackServer $server) use ($handler, &$statusReceived, $data){
            $handler->waitForResult($server)->then(function() use (&$statusReceived, $data){
                $statusReceived = new JobStatus($data);
            });

            $server->playback();
        });

        $this->assertInstanceOf(JobStatus::class, $statusReceived);
        $this->assertEquals('H:test:1', $statusReceived->getJobHandle());
        $this->assertEquals(5, $statusReceived->getNumerator());
        $this->assertEquals(10, $statusReceived->getDenominator());
        $this->assertTrue($statusReceived->isKnown());
        $this->assertTrue($statusReceived->isRunning());
        $this->assertFalse($server->hasHandler($handler));
    }

    public function testStatusReceivedIsCorrectHandle(){
        $server = new PacketPlaybackServer([
            new IncomingPacket(PacketMagic::REQ, PacketType::GET_STATUS, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::STATUS_RES, ['H:test:2', 1, 1, 5, 10])
            , new OutgoingPacket(PacketMagic::RES, PacketType::STATUS_RES, ['H:test:1', 1, 0, 0, 10])
        ]);

        $data = new JobStatusData('H:test:1');
        $handler = new JobStatusHandler($data);

        $statusReceived = null;
        $server->connect()->then(function(PacketPlaybackServer $server) use ($handler, &$statusReceived, $data){
            $handler->waitForResult($server)->then(function() use (&$statusReceived, $data){
                $statusReceived = new JobStatus($data);
            });
            $server->playback();
        });

        $this->assertInstanceOf(JobStatus::class, $statusReceived);
        $this->assertEquals('H:test:1', $statusReceived->getJobHandle());
        $this->assertEquals(0, $statusReceived->getNumerator());
        $this->assertEquals(10, $statusReceived->getDenominator());
        $this->assertTrue($statusReceived->isKnown());
        $this->assertFalse($statusReceived->isRunning());
        $this->assertFalse($server->hasHandler($handler));
    }
}