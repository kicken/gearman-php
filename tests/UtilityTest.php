<?php

namespace Kicken\Gearman\Test;

use Kicken\Gearman\Network\GearmanEndpoint;
use Kicken\Gearman\ServiceContainer;
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
        $mapped = mapToEndpointObjects($serverList, new ServiceContainer());

        $this->assertCount(1, $mapped);
        $this->assertInstanceOf(GearmanEndpoint::class, $mapped[0]);
    }

    public function testMapServerArrayToServerArray(){
        $serviceContainer = new ServiceContainer();
        $serverList = [new GearmanEndpoint('127.0.0.1:4730', null, $serviceContainer)];
        $mapped = mapToEndpointObjects($serverList, $serviceContainer);

        $this->assertCount(1, $mapped);
        $this->assertInstanceOf(GearmanEndpoint::class, $mapped[0]);
    }

    public function testMapServerWithInvalidType(){
        $serverList = [null];

        $this->expectException(\InvalidArgumentException::class);
        mapToEndpointObjects($serverList, new ServiceContainer());
    }
}
