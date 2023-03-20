<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\Packet;

interface PacketHandler {
    public function handlePacket(Endpoint $connection, Packet $packet) : bool;
}
