<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Network\Server;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\Packet;

abstract class BinaryPacketHandler implements PacketHandler {
    abstract public function handleBinaryPacket(Server $server, BinaryPacket $packet) : bool;

    public function handlePacket(?Server $server, Packet $packet) : bool{
        if ($packet instanceof BinaryPacket){
            return $this->handleBinaryPacket($server, $packet);
        } else {
            return false;
        }
    }
}