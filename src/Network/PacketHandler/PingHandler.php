<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;

class PingHandler extends BinaryPacketHandler {
    private Deferred $deferred;

    public function __construct(){
        $this->deferred = new Deferred();
    }

    public function handleBinaryPacket(Connection $connection, BinaryPacket $packet) : bool{
        if ($packet->getType() === PacketType::ECHO_RES){
            $connection->removePacketHandler($this);

            $sentTime = floatval($packet->getArgument(0));
            $endTime = round(microtime(true), 6);
            $delay = $endTime - $sentTime;
            $this->deferred->resolve($delay);

            return true;
        }

        return false;
    }

    public function ping(Connection $connection) : ExtendedPromiseInterface{
        $time = sprintf('%0.6f', microtime(true));
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::ECHO_REQ, [$time]);
        $connection->writePacket($packet);
        $connection->addPacketHandler($this);

        return $this->deferred->promise();
    }
}
