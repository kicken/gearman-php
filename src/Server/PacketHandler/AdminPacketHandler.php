<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\AdministrativePacketHandler;
use Kicken\Gearman\Protocol\AdministrativePacket;
use Kicken\Gearman\Server\Statistics;
use Psr\Log\LoggerInterface;

class AdminPacketHandler extends AdministrativePacketHandler {
    private LoggerInterface $logger;
    private Statistics $statistics;

    public function __construct(Statistics $statistics, LoggerInterface $logger){
        $this->logger = $logger;
        $this->statistics = $statistics;
    }

    public function handleAdministrativePacket(Connection $connection, AdministrativePacket $packet) : bool{
        switch ($packet->getCommand()){
            case 'workers':
                $this->logger->info('Handling workers admin command.');
                $connection->writePacket(new AdministrativePacket($this->statistics->listWorkerDetails()));
                break;
            case 'version':
                $this->logger->info('Handling version admin command.');
                $connection->writePacket(new AdministrativePacket('v0.0.1'));
                break;
            case 'status':
                $this->logger->info('Handling status admin commend.');
                $connection->writePacket(new AdministrativePacket($this->statistics->listQueueDetails()));
                break;
            default:
                return false;
        }

        return true;
    }
}