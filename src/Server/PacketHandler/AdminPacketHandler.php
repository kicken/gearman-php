<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\AdministrativePacketHandler;
use Kicken\Gearman\Protocol\AdministrativeCommandPacket;
use Kicken\Gearman\Protocol\AdministrativePacket;
use Kicken\Gearman\Server;
use Kicken\Gearman\Server\Statistics;
use Kicken\Gearman\ServiceContainer;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class AdminPacketHandler extends AdministrativePacketHandler {
    private Server $server;
    private LoggerInterface $logger;
    private ServiceContainer $services;

    public function __construct(Server $server, ServiceContainer $container){
        $this->logger = $logger ?? new NullLogger();
        $this->services = $container;
        $this->server = $server;
    }

    public function handleAdministrativePacket(Endpoint $connection, AdministrativePacket $packet) : bool{
        if (!$packet instanceof AdministrativeCommandPacket){
            return false;
        }

        switch ($packet->getCommand()){
            case 'workers':
                $this->logger->info('Handling workers admin command.');
                $connection->writePacket(new AdministrativePacket($this->statistics()->listWorkerDetails()));
                break;
            case 'version':
                $this->logger->info('Handling version admin command.');
                $connection->writePacket(new AdministrativePacket('v0.0.1'));
                break;
            case 'status':
                $this->logger->info('Handling status admin commend.');
                $connection->writePacket(new AdministrativePacket($this->statistics()->listQueueDetails()));
                break;
            case 'shutdown':
                $this->logger->info('Handling shutdown admin command.');
                $this->server->shutdown($packet->getArgument(1) === 'graceful');
                $connection->disconnect();
                break;
            default:
                return false;
        }

        return true;
    }

    private function statistics() : Statistics{
        return $this->services->statistics;
    }
}
