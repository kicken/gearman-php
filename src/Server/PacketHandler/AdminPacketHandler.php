<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\AdministrativePacketHandler;
use Kicken\Gearman\Protocol\AdministrativePacket;
use Kicken\Gearman\Server\WorkerManager;
use Psr\Log\LoggerInterface;

class AdminPacketHandler extends AdministrativePacketHandler {
    private WorkerManager $workerRegistry;
    private LoggerInterface $logger;

    public function __construct(WorkerManager $registry, LoggerInterface $logger){
        $this->workerRegistry = $registry;
        $this->logger = $logger;
    }

    public function handleAdministrativePacket(Connection $connection, AdministrativePacket $packet) : bool{
        switch ($packet->getCommand()){
            case 'workers':
                $this->logger->info('Handling workers admin command.');
                $connection->writePacket(new AdministrativePacket($this->workerRegistry->listWorkerDetails()));
                break;
            case 'version':
                $this->logger->info('Handling version admin command.');
                $connection->writePacket(new AdministrativePacket('v0.0.1'));
                break;
            default:
                return false;
        }

        return true;
    }
}