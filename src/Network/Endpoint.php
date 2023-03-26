<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use Kicken\Gearman\Protocol\Packet;
use React\Promise\PromiseInterface;

interface Endpoint {
    public function connect(bool $autoDisconnect) : PromiseInterface;

    public function disconnect() : void;

    public function getFd() : int;

    public function getAddress() : string;

    public function listen(callable $handler) : void;

    public function shutdown() : void;

    public function on(string $event, callable $callback) : void;

    public function writePacket(Packet $packet) : void;

    public function addPacketHandler(PacketHandler $handler) : void;

    public function removePacketHandler(PacketHandler $handler) : void;
}
