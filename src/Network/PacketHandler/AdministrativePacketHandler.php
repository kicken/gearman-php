<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Protocol\AdministrativePacket;
use Kicken\Gearman\Protocol\Packet;

abstract class AdministrativePacketHandler implements PacketHandler {
    abstract public function handleAdministrativePacket(Connection $connection, AdministrativePacket $packet) : bool;

    public function handlePacket(Connection $server, Packet $packet) : bool{
        if ($packet instanceof AdministrativePacket){
            return $this->handleAdministrativePacket($server, $packet);
        } else {
            return false;
        }
    }
}