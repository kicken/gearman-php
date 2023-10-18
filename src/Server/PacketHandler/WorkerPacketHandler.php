<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server;
use Kicken\Gearman\ServiceContainer;

class WorkerPacketHandler extends BinaryPacketHandler {
    private Server $server;
    private ServiceContainer $services;

    public function __construct(Server $server, ServiceContainer $container){
        $this->server = $server;
        $this->services = $container;
    }

    public function handleBinaryPacket(Endpoint $connection, BinaryPacket $packet) : bool{
        switch ($packet->getType()){
            case PacketType::CAN_DO:
                $function = $packet->getArgument(0);
                $this->services->logger->info('Registering worker function.', [
                    'worker' => $connection->getAddress(),
                    'function' => $function
                ]);
                $this->services->workerManager->getWorker($connection)->registerFunction($function);
                break;
            case PacketType::CAN_DO_TIMEOUT:
                $function = $packet->getArgument(0);
                $timeout = $packet->getArgument(1);
                $this->services->logger->info('Registering worker function.', [
                    'worker' => $connection->getAddress(),
                    'function' => $function, 'timeout' => $timeout
                ]);
                $this->services->workerManager->getWorker($connection)->registerFunction($function, $timeout);
                break;
            case PacketType::PRE_SLEEP:
                $worker = $this->services->workerManager->getWorker($connection);
                if ($this->services->jobQueue->hasJobFor($worker)){
                    $this->services->logger->info('Worker went to sleep, but work is available.', ['worker' => $connection->getAddress()]);
                    $worker->wake();
                } else {
                    $this->services->logger->info('Worker is going to sleep.', ['worker' => $connection->getAddress()]);
                    $worker->sleep();
                }
                break;
            case PacketType::GRAB_JOB:
            case PacketType::GRAB_JOB_UNIQ:
            case PacketType::GRAB_JOB_ALL:
                $this->services->logger->info('Worker is requesting a job.', ['worker' => $connection->getAddress()]);
                $this->assignJob($connection, $packet->getType());
                break;
            case PacketType::WORK_DATA:
            case PacketType::WORK_WARNING:
                $this->services->logger->info('Worker data/warning event.', [
                    'worker' => $connection->getAddress()
                    , 'type' => $packet->getType()
                ]);
                $job = $this->services->workerManager->getWorker($connection)->getCurrentJob();
                $job->sendToWatchers(new BinaryPacket(PacketMagic::RES, $packet->getType(), $packet->getArgumentList()));
                break;
            case PacketType::WORK_STATUS:
                $this->services->logger->info('Worker status event.', [
                    'worker' => $connection->getAddress()
                    , 'type' => $packet->getType()
                ]);
                $job = $this->services->workerManager->getWorker($connection)->getCurrentJob();
                $job->numerator = $packet->getArgument(1);
                $job->denominator = $packet->getArgument(2);
                $job->sendToWatchers(new BinaryPacket(PacketMagic::RES, $packet->getType(), $packet->getArgumentList()));
                break;
            case PacketType::WORK_FAIL:
            case PacketType::WORK_COMPLETE:
            case PacketType::WORK_EXCEPTION:
                $this->services->logger->info('Worker fail/complete/exception event.', [
                    'worker' => $connection->getAddress()
                    , 'type' => $packet->getType()
                ]);
                $worker = $this->services->workerManager->getWorker($connection);
                $job = $worker->getCurrentJob();
                if ($job){
                    $job->running = false;
                    $job->sendToWatchers(new BinaryPacket(PacketMagic::RES, $packet->getType(), $packet->getArgumentList()));
                    $worker->assignJob(null);
                    $this->services->jobQueue->setComplete($job);
                }
                break;
            default:
                return false;
        }

        return true;
    }

    private function assignJob(Endpoint $connection, int $grabType) : void{
        if ($this->server->isShutdown()){
            $connection->disconnect();
        }

        $worker = $this->services->workerManager->getWorker($connection);
        $this->services->logger->debug('Worker function list', [
            'functions' => $worker->getAvailableFunctions()
        ]);
        $job = $this->services->jobQueue->dequeue($worker->getAvailableFunctions());
        if (!$job){
            $this->services->logger->info('No job available for worker.');
            $connection->writePacket(new BinaryPacket(PacketMagic::RES, PacketType::NO_JOB));
        } else {
            $job->running = true;
            $this->services->logger->info('Assigning job to worker', [
                'worker' => $connection->getAddress()
                , 'job' => $job->jobHandle
            ]);
            $assignType = PacketType::JOB_ASSIGN;
            $arguments = [$job->jobHandle, $job->function];
            if ($grabType === PacketType::GRAB_JOB_UNIQ){
                $arguments[] = $job->uniqueId;
                $assignType = PacketType::JOB_ASSIGN_UNIQ;
            } else if ($grabType === PacketType::GRAB_JOB_ALL){
                $arguments[] = $job->uniqueId;
                $arguments[] = $job->reducer;
                $assignType = PacketType::JOB_ASSIGN_ALL;
            }
            $arguments[] = $job->workload;
            $connection->writePacket(new BinaryPacket(PacketMagic::RES, $assignType, $arguments));
            $worker->assignJob($job);
            $this->services->jobQueue->setRunning($job);
        }
    }
}
