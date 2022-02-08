<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\Packet;

abstract class BinaryPacketHandler implements PacketHandler {
    abstract public function handleBinaryPacket(Connection $connection, BinaryPacket $packet) : bool;

    public function handlePacket(?Connection $connection, Packet $packet) : bool{
        if ($packet instanceof BinaryPacket){
            return $this->handleBinaryPacket($connection, $packet);
        } else {
            return false;
        }
    }
}