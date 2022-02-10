<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\AdministrativePacketHandler;
use Kicken\Gearman\Protocol\AdministrativePacket;
use Kicken\Gearman\Server\WorkerRegistry;

class AdminPacketHandler extends AdministrativePacketHandler {
    private WorkerRegistry $workerRegistry;

    public function __construct(WorkerRegistry $registry){
        $this->workerRegistry = $registry;
    }

    public function handleAdministrativePacket(Connection $connection, AdministrativePacket $packet) : bool{
        switch ($packet->getCommand()){
            case 'workers':
                $connection->writePacket(new AdministrativePacket($this->workerRegistry->listWorkerDetails()));
                break;
            case 'version':
                $connection->writePacket(new AdministrativePacket('v0.0.1'));
                break;
            default:
                return false;
        }

        return true;
    }
}