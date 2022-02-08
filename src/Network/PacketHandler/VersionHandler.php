<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Protocol\AdministrativePacket;

class VersionHandler extends AdministrativePacketHandler {
    public function handleAdministrativePacket(Connection $connection, AdministrativePacket $packet) : bool{
        if ($packet->getCommand() !== 'version'){
            return false;
        }

        $connection->writePacket(new AdministrativePacket('v0.0.1'));

        return true;
    }
}
