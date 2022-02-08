<?php

namespace Kicken\Gearman\Test;

use Kicken\Gearman\Network\GearmanEndpoint;
use PHPUnit\Framework\TestCase;
use function Kicken\Gearman\fromBigEndian;
use function Kicken\Gearman\mapToEndpointObjects;
use function Kicken\Gearman\toBigEndian;

class UtilityTest extends TestCase {
    public function testFromBigEndian(){
        $big = "\x1\x2\x3\x4";

        $this->assertEquals(0x01020304, fromBigEndian($big));
    }

    public function testToBigEndian(){
        $native = 0x01020304;

        $this->assertEquals("\x1\x2\x3\x4", toBigEndian($native));
    }

    public function testMapStringArrayToServerArray(){
        $serverList = ['127.0.0.1:4730'];
        $mapped = mapToEndpointObjects($serverList, null);

        $this->assertCount(1, $mapped);
        $this->assertInstanceOf(GearmanEndpoint::class, $mapped[0]);
    }

    public function testMapServerArrayToServerArray(){
        $serverList = [new GearmanEndpoint('127.0.0.1:4730')];
        $mapped = mapToEndpointObjects($serverList, null);

        $this->assertCount(1, $mapped);
        $this->assertInstanceOf(GearmanEndpoint::class, $mapped[0]);
    }

    public function testMapServerWithInvalidType(){
        $serverList = [null];

        $this->expectException(\InvalidArgumentException::class);
        mapToEndpointObjects($serverList, null);
    }
}
