<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\WorkerRegistry;

class WorkerRegistrationHandler extends BinaryPacketHandler {
    private WorkerRegistry $registry;

    public function __construct(WorkerRegistry $registry){
        $this->registry = $registry;
    }

    public function handleBinaryPacket(Connection $connection, BinaryPacket $packet) : bool{
        switch ($packet->getType()){
            case PacketType::CAN_DO:
            case PacketType::CAN_DO_TIMEOUT:
                $this->registerFunction($connection, $packet);

                return true;
            case PacketType::CANT_DO:
                $this->unregisterFunction($connection, $packet);

                return true;
        }

        return false;
    }

    private function registerFunction(Connection $connection, BinaryPacket $packet){
        $timeout = $packet->getType() === PacketType::CAN_DO_TIMEOUT ? $packet->getArgument(1) : null;

        $worker = $this->registry->getWorker($connection) ?? $this->registry->createWorker($connection);
        $worker->registerFunction($packet->getArgument(0), $timeout);
    }

    private function unregisterFunction(Connection $connection, BinaryPacket $packet){
        $worker = $this->registry->getWorker($connection);
        if ($worker){
            $worker->unregisterFunction($packet->getArgument(0));
        }
    }
}
