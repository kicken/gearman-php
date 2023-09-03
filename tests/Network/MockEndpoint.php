<?php

namespace Kicken\Gearman\Test\Network;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use Kicken\Gearman\Protocol\Packet;
use React\Promise\PromiseInterface;
use function React\Promise\resolve;

class MockEndpoint implements Endpoint {
    private array $writtenPackets = [];

    public function connect() : PromiseInterface{
        return resolve($this);
    }

    public function disconnect() : void{
    }

    public function getFd() : int{
        return 1;
    }

    public function getAddress() : string{
        return '0.0.0.0:4730';
    }

    public function listen(callable $handler) : void{
    }

    public function shutdown() : void{
    }

    public function on(string $event, callable $callback) : void{
    }

    public function writePacket(Packet $packet) : void{
        $this->writtenPackets[] = $packet;
    }

    public function addPacketHandler(PacketHandler $handler) : void{
    }

    public function removePacketHandler(PacketHandler $handler) : void{
    }

    public function setClientId(string $clientId){
    }

    public function getClientId() : string{
        return 'test';
    }

    public function setOption(string $option) : bool{
        return true;
    }
}