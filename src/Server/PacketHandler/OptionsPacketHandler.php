<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;

class OptionsPacketHandler extends BinaryPacketHandler {
    public function handleBinaryPacket(Endpoint $connection, BinaryPacket $packet) : bool{
        if ($packet->getType() === PacketType::OPTION_REQ){
            $option = $packet->getArgument(0);
            if ($connection->setOption($option)){
                $packet = new BinaryPacket(PacketMagic::RES, PacketType::OPTION_RES, [$option]);
            } else {
                $packet = new BinaryPacket(PacketMagic::RES, PacketType::ERROR);
            }
            $connection->writePacket($packet);

            return true;
        }

        return false;
    }
}