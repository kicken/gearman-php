<?php

namespace Kicken\Gearman\Client\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;

class PingHandler extends BinaryPacketHandler {
    private Deferred $deferred;
    private LoggerInterface $logger;

    public function __construct(?LoggerInterface $logger = null){
        $this->deferred = new Deferred();
        $this->logger = $logger ?? new NullLogger();
    }

    public function handleBinaryPacket(Endpoint $connection, BinaryPacket $packet) : bool{
        if ($packet->getType() === PacketType::ECHO_RES){
            $connection->removePacketHandler($this);

            $sentTime = floatval($packet->getArgument(0));
            $endTime = round(microtime(true), 6);
            $delay = $endTime - $sentTime;

            $this->logger->debug('Received ping from server', ['server' => $connection->getAddress(), 'delay' => $delay]);
            $this->deferred->resolve($delay);

            return true;
        }

        return false;
    }

    public function ping(Endpoint $connection) : ExtendedPromiseInterface{
        $this->logger->debug('Sending ping to server', ['server' => $connection->getAddress()]);
        $time = sprintf('%0.6f', microtime(true));
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::ECHO_REQ, [$time]);
        $connection->writePacket($packet);
        $connection->addPacketHandler($this);

        return $this->deferred->promise();
    }
}
