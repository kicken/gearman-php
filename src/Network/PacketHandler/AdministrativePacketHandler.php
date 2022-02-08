<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Network\Server;
use Kicken\Gearman\Protocol\AdministrativePacket;
use Kicken\Gearman\Protocol\Packet;

abstract class AdministrativePacketHandler implements PacketHandler {
    abstract public function handleAdministrativePacket(AdministrativePacket $packet) : bool;

    public function handlePacket(?Server $server, Packet $packet) : bool{
        if ($packet instanceof AdministrativePacket){
            return $this->handleAdministrativePacket($packet);
        } else {
            return false;
        }
    }
}