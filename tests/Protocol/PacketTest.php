<?php

namespace Kicken\Gearman\Test\Protocol;

use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use PHPUnit\Framework\TestCase;

class PacketTest extends TestCase {
    public function testStringifyCanDo(){
        $packet = new Packet(PacketMagic::REQ, PacketType::CAN_DO, ['reverse']);
        $this->assertEquals("\x0REQ\x0\x0\x0\x1\x0\x0\x0\x7reverse", (string)$packet);
    }

    public function testStringifyGrabJob(){
        $packet = new Packet(PacketMagic::REQ, PacketType::GRAB_JOB, []);
        $this->assertEquals("\x0REQ\x0\x0\x0\x9\x0\x0\x0\x0", (string)$packet);
    }

    public function testStringifyNoJob(){
        $packet = new Packet(PacketMagic::RES, PacketType::NO_JOB, []);
        $this->assertEquals("\x0RES\x0\x0\x0\xa\x0\x0\x0\x0", (string)$packet);

    }

    public function testStringifySubmitJob(){
        $packet = new Packet(PacketMagic::REQ, PacketType::SUBMIT_JOB, ['reverse', '', 'test']);
        $this->assertEquals("\x0REQ\x0\x0\x0\x7\x0\x0\x0\xdreverse\x0\x0test", (string)$packet);
    }

    public function testStringifyJobCreated(){
        $packet = new Packet(PacketMagic::RES, PacketType::JOB_CREATED, ['H:lap:1']);
        $this->assertEquals("\x0RES\x0\x0\x0\x8\x0\x0\x0\x7H:lap:1", (string)$packet);
    }

    public function testStringifyNoop(){
        $packet = new Packet(PacketMagic::RES, PacketType::NOOP, ['']);
        $this->assertEquals("\x0RES\x0\x0\x0\x6\x0\x0\x0\x0", (string)$packet);
    }
}
