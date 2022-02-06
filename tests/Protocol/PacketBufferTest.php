<?php

namespace Kicken\Gearman\Test\Protocol;

use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketBuffer;
use Kicken\Gearman\Protocol\PacketType;
use PHPUnit\Framework\TestCase;

class PacketBufferTest extends TestCase {
    public function testParseCanDo(){
        $buffer = $this->createBuffer("\x0REQ\x0\x0\x0\x1\x0\x0\x0\x7reverse");
        $packet = $buffer->readPacket();

        $this->assertInstanceOf(Packet::class, $packet);
        $this->assertEquals(PacketType::CAN_DO, $packet->getType());
        $this->assertEquals('reverse', $packet->getArgument(0));
    }

    public function testParseGrabJob(){
        $buffer = $this->createBuffer("\x0REQ\x0\x0\x0\x9\x0\x0\x0\x0");
        $packet = $buffer->readPacket();

        $this->assertInstanceOf(Packet::class, $packet);
        $this->assertEquals(PacketType::GRAB_JOB, $packet->getType());
    }

    public function testParseNoJob(){
        $buffer = $this->createBuffer("\x0RES\x0\x0\x0\xa\x0\x0\x0\x0");
        $packet = $buffer->readPacket();

        $this->assertInstanceOf(Packet::class, $packet);
        $this->assertEquals(PacketType::NO_JOB, $packet->getType());
    }

    public function testParseSubmitJob(){
        $buffer = $this->createBuffer("\x0REQ\x0\x0\x0\x7\x0\x0\x0\xdreverse\x0\x0test");
        $packet = $buffer->readPacket();

        $this->assertInstanceOf(Packet::class, $packet);
        $this->assertEquals(PacketType::SUBMIT_JOB, $packet->getType());
        $this->assertEquals('reverse', $packet->getArgument(0));
        $this->assertEquals('', $packet->getArgument(1));
        $this->assertEquals('test', $packet->getArgument(2));
    }

    public function testParseJobCreated(){
        $buffer = $this->createBuffer("\x0RES\x0\x0\x0\x8\x0\x0\x0\x7H:lap:1");
        $packet = $buffer->readPacket();

        $this->assertInstanceOf(Packet::class, $packet);
        $this->assertEquals(PacketType::JOB_CREATED, $packet->getType());
        $this->assertEquals('H:lap:1', $packet->getArgument(0));
    }

    public function testParseNoop(){
        $buffer = $this->createBuffer("\x0RES\x0\x0\x0\x6\x0\x0\x0\x0");
        $packet = $buffer->readPacket();

        $this->assertInstanceOf(Packet::class, $packet);
        $this->assertEquals(PacketType::NOOP, $packet->getType());
    }

    private function createBuffer(string $data) : PacketBuffer{
        $buffer = new PacketBuffer();
        $buffer->feed($data);

        return $buffer;
    }
}