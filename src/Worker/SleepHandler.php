<?php

namespace Kicken\Gearman\Worker;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Psr\Log\LoggerInterface;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;

class SleepHandler extends BinaryPacketHandler {
    private Deferred $deferred;
    private LoggerInterface $logger;

    public function __construct(LoggerInterface $logger){
        $this->logger = $logger;
    }

    public function sleep(Endpoint $server) : ExtendedPromiseInterface{
        $this->deferred = new Deferred();
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::PRE_SLEEP);
        $server->writePacket($packet);
        $server->addPacketHandler($this);
        $this->deferred = new Deferred();

        return $this->deferred->promise();
    }

    public function handleBinaryPacket(Endpoint $connection, BinaryPacket $packet) : bool{
        if ($packet->getType() === PacketType::NOOP){
            $this->logger->debug('Server issued wakeup.', [
                'server' => $connection->getAddress()
            ]);
            $this->deferred->resolve();

            return true;
        }

        return false;
    }
}
