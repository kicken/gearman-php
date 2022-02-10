<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\WorkerRegistry;

class WorkerPacketHandler extends BinaryPacketHandler {
    private WorkerRegistry $registry;

    public function __construct(WorkerRegistry $registry){
        $this->registry = $registry;
    }

    public function handleBinaryPacket(Connection $connection, BinaryPacket $packet) : bool{
        switch ($packet->getType()){
            case PacketType::CAN_DO:
                $this->registerFunction($connection, $packet->getArgument(0));
                break;
            case PacketType::CAN_DO_TIMEOUT:
                $this->registerFunction($connection, $packet->getArgument(0), $packet->getArgument(1));
                break;
            default:
                return false;
        }

        return true;
    }

    private function registerFunction(Connection $connection, string $function, ?int $timeout = null){
        $this->registry->registerWorker($connection, $function, $timeout);
    }
}
