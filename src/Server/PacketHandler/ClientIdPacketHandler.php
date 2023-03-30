<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketType;

class ClientIdPacketHandler extends BinaryPacketHandler {
    public function handleBinaryPacket(Endpoint $connection, BinaryPacket $packet) : bool{
        if ($packet->getType() === PacketType::SET_CLIENT_ID){
            $connection->setClientId($packet->getArgument(0));

            return true;
        }

        return false;
    }
}
