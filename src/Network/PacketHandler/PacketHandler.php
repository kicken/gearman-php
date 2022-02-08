<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Protocol\Packet;

interface PacketHandler {
    public function handlePacket(Connection $connection, Packet $packet) : bool;
}
