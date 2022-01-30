<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Network\Server;
use Kicken\Gearman\Protocol\Packet;

interface PacketHandler {
    public function handlePacket(Server $server, Packet $packet) : bool;
}
