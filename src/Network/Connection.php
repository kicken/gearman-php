<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use Kicken\Gearman\Protocol\Packet;
use React\Promise\ExtendedPromiseInterface;

interface Connection {
    public function isConnected() : bool;

    public function writePacket(Packet $packet) : void;

    public function disconnect() : void;

    public function addPacketHandler(PacketHandler $handler) : void;

    public function removePacketHandler(PacketHandler $handler) : void;
}