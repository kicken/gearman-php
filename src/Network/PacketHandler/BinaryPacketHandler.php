<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\Packet;

abstract class BinaryPacketHandler implements PacketHandler {
    abstract public function handleBinaryPacket(Endpoint $connection, BinaryPacket $packet) : bool;

    public function handlePacket(?Endpoint $connection, Packet $packet) : bool{
        if ($packet instanceof BinaryPacket){
            return $this->handleBinaryPacket($connection, $packet);
        } else {
            return false;
        }
    }
}