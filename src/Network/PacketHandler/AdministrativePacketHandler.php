<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\AdministrativePacket;
use Kicken\Gearman\Protocol\Packet;

abstract class AdministrativePacketHandler implements PacketHandler {
    abstract public function handleAdministrativePacket(Endpoint $connection, AdministrativePacket $packet) : bool;

    public function handlePacket(Endpoint $connection, Packet $packet) : bool{
        if ($packet instanceof AdministrativePacket){
            return $this->handleAdministrativePacket($connection, $packet);
        } else {
            return false;
        }
    }
}