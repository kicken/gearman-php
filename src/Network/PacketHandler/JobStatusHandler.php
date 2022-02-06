<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Job\Data\JobStatusData;
use Kicken\Gearman\Network\Server;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;

class JobStatusHandler implements PacketHandler {
    private JobStatusData $data;
    private Deferred $deferred;

    public function __construct(JobStatusData $data){
        $this->data = $data;
        $this->deferred = new Deferred();
    }

    public function handlePacket(Server $server, Packet $packet) : bool{
        if ($packet->getType() === PacketType::STATUS_RES && $packet->getArgument(0) === $this->data->jobHandle){
            $this->data->isKnown = (bool)(int)$packet->getArgument(1);
            $this->data->isRunning = (bool)(int)$packet->getArgument(2);
            $this->data->numerator = (int)$packet->getArgument(3);
            $this->data->denominator = (int)$packet->getArgument(4);
            $this->deferred->resolve();
            $server->removePacketHandler($this);

            return true;
        }

        return false;
    }

    public function waitForResult(Server $server) : PromiseInterface{
        $packet = new Packet(PacketMagic::REQ, PacketType::GET_STATUS, [$this->data->jobHandle]);
        $server->writePacket($packet);
        $server->addPacketHandler($this);

        return $this->deferred->promise();
    }
}
