<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server;
use Kicken\Gearman\Server\JobQueue;
use Kicken\Gearman\Server\WorkerManager;
use Psr\Log\LoggerInterface;

class WorkerPacketHandler extends BinaryPacketHandler {
    private Server $server;
    private WorkerManager $workerManager;
    private JobQueue $jobQueue;
    private LoggerInterface $logger;

    public function __construct(Server $server, WorkerManager $manager, JobQueue $jobQueue, LoggerInterface $logger){
        $this->server = $server;
        $this->workerManager = $manager;
        $this->jobQueue = $jobQueue;
        $this->logger = $logger;
    }

    public function handleBinaryPacket(Connection $connection, BinaryPacket $packet) : bool{
        switch ($packet->getType()){
            case PacketType::CAN_DO:
                $function = $packet->getArgument(0);
                $this->logger->info('Registering worker function.', [
                    'worker' => $connection->getRemoteAddress(),
                    'function' => $function
                ]);
                $this->workerManager->getWorker($connection)->registerFunction($function);
                break;
            case PacketType::CAN_DO_TIMEOUT:
                $function = $packet->getArgument(0);
                $timeout = $packet->getArgument(1);
                $this->logger->info('Registering worker function.', [
                    'worker' => $connection->getRemoteAddress(),
                    'function' => $function, 'timeout' => $timeout
                ]);
                $this->workerManager->getWorker($connection)->registerFunction($function, $timeout);
                break;
            case PacketType::PRE_SLEEP:
                $this->logger->debug('Worker is going to sleep.', ['worker' => $connection->getRemoteAddress()]);
                $this->workerManager->getWorker($connection)->sleep();
                break;
            case PacketType::GRAB_JOB:
            case PacketType::GRAB_JOB_UNIQ:
                $this->logger->debug('Worker is requesting a job.', ['worker' => $connection->getRemoteAddress()]);
                $this->assignJob($connection, $packet->getType());
                break;
            case PacketType::WORK_DATA:
            case PacketType::WORK_WARNING:
                $this->logger->debug('Worker data/warning event.', [
                    'worker' => $connection->getRemoteAddress()
                    , 'type' => $packet->getType()
                ]);
                $job = $this->workerManager->getWorker($connection)->getCurrentJob();
                $job->sendToWatchers(new BinaryPacket(PacketMagic::RES, $packet->getType(), $packet->getArgumentList()));
                break;
            case PacketType::WORK_STATUS:
                $this->logger->debug('Worker status event.', [
                    'worker' => $connection->getRemoteAddress()
                    , 'type' => $packet->getType()
                ]);
                $job = $this->workerManager->getWorker($connection)->getCurrentJob();
                $job->numerator = $packet->getArgument(1);
                $job->denominator = $packet->getArgument(2);
                $job->sendToWatchers(new BinaryPacket(PacketMagic::RES, $packet->getType(), $packet->getArgumentList()));
                break;
            case PacketType::WORK_FAIL:
            case PacketType::WORK_COMPLETE:
            case PacketType::WORK_EXCEPTION:
                $this->logger->debug('Worker fail/complete/exception event.', [
                    'worker' => $connection->getRemoteAddress()
                    , 'type' => $packet->getType()
                ]);
                $worker = $this->workerManager->getWorker($connection);
                $job = $worker->getCurrentJob();
                if ($job){
                    $this->jobQueue->deleteJob($job);
                    $job->running = false;
                    $job->sendToWatchers(new BinaryPacket(PacketMagic::RES, $packet->getType(), $packet->getArgumentList()));
                    $worker->assignJob(null);
                }
                break;
            default:
                return false;
        }

        return true;
    }

    private function assignJob(Connection $connection, int $grabType){
        $worker = $this->workerManager->getWorker($connection);
        $job = $this->jobQueue->assignJob($worker);
        if (!$job){
            if ($this->server->isShutdown()){
                $connection->disconnect();
            } else {
                $connection->writePacket(new BinaryPacket(PacketMagic::RES, PacketType::NO_JOB));
            }
        } else {
            $job->running = true;
            $this->logger->info('Assigning job to worker', [
                'worker' => $connection->getRemoteAddress()
                , 'job' => $job->jobHandle
            ]);
            if ($grabType === PacketType::GRAB_JOB){
                $connection->writePacket(new BinaryPacket(PacketMagic::RES, PacketType::JOB_ASSIGN, [
                    $job->jobHandle
                    , $job->function
                    , $job->workload
                ]));
            } else {
                $connection->writePacket(new BinaryPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, [
                    $job->jobHandle
                    , $job->function
                    , $job->uniqueId
                    , $job->workload
                ]));
            }
        }
    }
}
