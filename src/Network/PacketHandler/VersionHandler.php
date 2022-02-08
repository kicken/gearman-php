<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Protocol\AdministrativePacket;

class VersionHandler extends AdministrativePacketHandler {
    public function handleAdministrativePacket(AdministrativePacket $packet) : bool{
        if ($packet->getCommand() !== 'version'){
            return false;
        }

        echo 'Version packet received';

        return true;
    }
}
