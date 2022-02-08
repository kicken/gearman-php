<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\WorkerConnection;
use Kicken\Gearman\Server\WorkerRegistry;

class WorkerJobHandler extends BinaryPacketHandler {
    private WorkerRegistry $workerRegistry;

    public function __construct(WorkerRegistry $workerRegistry){
        $this->workerRegistry = $workerRegistry;
    }

    public function handleBinaryPacket(Connection $connection, BinaryPacket $packet) : bool{
        $worker = $this->workerRegistry->getWorker($connection);
        if (!$worker){
            return false;
        }

        switch ($packet->getType()){
            case PacketType::GRAB_JOB:
            case PacketType::GRAB_JOB_UNIQ:
                $this->grabJob($worker);

                return true;
            case PacketType::PRE_SLEEP:
                $worker->isSleeping(true);

                return true;
            default:
                return false;
        }
    }

    public function grabJob(WorkerConnection $worker){
        $worker->send(new BinaryPacket(PacketMagic::RES, PacketType::NO_JOB));
    }
}
