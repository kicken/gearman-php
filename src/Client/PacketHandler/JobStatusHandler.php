<?php

namespace Kicken\Gearman\Client\PacketHandler;

use Kicken\Gearman\Job\Data\JobStatusData;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;

class JobStatusHandler extends BinaryPacketHandler {
    private JobStatusData $data;
    private Deferred $deferred;
    private LoggerInterface $logger;

    public function __construct(JobStatusData $data, ?LoggerInterface $logger = null){
        $this->data = $data;
        $this->deferred = new Deferred();
        $this->logger = $logger ?? new NullLogger();
    }

    public function handleBinaryPacket(Endpoint $connection, BinaryPacket $packet) : bool{
        if ($packet->getType() === PacketType::STATUS_RES && $packet->getArgument(0) === $this->data->jobHandle){
            $this->logger->info('Job status updated', [
                'server' => $connection->getAddress()
                , 'handle' => $this->data->jobHandle
            ]);
            $this->data->isKnown = (bool)(int)$packet->getArgument(1);
            $this->data->isRunning = (bool)(int)$packet->getArgument(2);
            $this->data->numerator = (int)$packet->getArgument(3);
            $this->data->denominator = (int)$packet->getArgument(4);
            $this->deferred->resolve();
            $connection->removePacketHandler($this);

            return true;
        }

        return false;
    }

    public function waitForResult(Endpoint $server) : ExtendedPromiseInterface{
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::GET_STATUS, [$this->data->jobHandle]);
        $server->writePacket($packet);
        $server->addPacketHandler($this);

        $this->logger->info('Requesting job status', [
            'server' => $server->getAddress()
            , 'handle' => $this->data->jobHandle
        ]);

        return $this->deferred->promise();
    }
}
