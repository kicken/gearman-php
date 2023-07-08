<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server;
use Kicken\Gearman\Server\JobQueue\JobQueue;
use Kicken\Gearman\Server\WorkerManager;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class WorkerPacketHandler extends BinaryPacketHandler {
    private Server $server;
    private WorkerManager $workerManager;
    private JobQueue $jobQueue;
    private LoggerInterface $logger;

    public function __construct(Server $server, WorkerManager $manager, JobQueue $jobQueue, ?LoggerInterface $logger = null){
        $this->server = $server;
        $this->workerManager = $manager;
        $this->jobQueue = $jobQueue;
        $this->logger = $logger ?? new NullLogger();
    }

    public function handleBinaryPacket(Endpoint $connection, BinaryPacket $packet) : bool{
        switch ($packet->getType()){
            case PacketType::CAN_DO:
                $function = $packet->getArgument(0);
                $this->logger->info('Registering worker function.', [
                    'worker' => $connection->getAddress(),
                    'function' => $function
                ]);
                $this->workerManager->getWorker($connection)->registerFunction($function);
                break;
            case PacketType::CAN_DO_TIMEOUT:
                $function = $packet->getArgument(0);
                $timeout = $packet->getArgument(1);
                $this->logger->info('Registering worker function.', [
                    'worker' => $connection->getAddress(),
                    'function' => $function, 'timeout' => $timeout
                ]);
                $this->workerManager->getWorker($connection)->registerFunction($function, $timeout);
                break;
            case PacketType::PRE_SLEEP:
                $worker = $this->workerManager->getWorker($connection);
                if ($this->jobQueue->hasJobFor($worker)){
                    $this->logger->info('Worker went to sleep, but work is available.', ['worker' => $connection->getAddress()]);
                    $worker->wake();
                } else {
                    $this->logger->info('Worker is going to sleep.', ['worker' => $connection->getAddress()]);
                    $worker->sleep();
                }
                break;
            case PacketType::GRAB_JOB:
            case PacketType::GRAB_JOB_UNIQ:
            case PacketType::GRAB_JOB_ALL:
                $this->logger->info('Worker is requesting a job.', ['worker' => $connection->getAddress()]);
                $this->assignJob($connection, $packet->getType());
                break;
            case PacketType::WORK_DATA:
            case PacketType::WORK_WARNING:
                $this->logger->info('Worker data/warning event.', [
                    'worker' => $connection->getAddress()
                    , 'type' => $packet->getType()
                ]);
                $job = $this->workerManager->getWorker($connection)->getCurrentJob();
                $job->sendToWatchers(new BinaryPacket(PacketMagic::RES, $packet->getType(), $packet->getArgumentList()));
                break;
            case PacketType::WORK_STATUS:
                $this->logger->info('Worker status event.', [
                    'worker' => $connection->getAddress()
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
                $this->logger->info('Worker fail/complete/exception event.', [
                    'worker' => $connection->getAddress()
                    , 'type' => $packet->getType()
                ]);
                $worker = $this->workerManager->getWorker($connection);
                $job = $worker->getCurrentJob();
                if ($job){
                    $job->running = false;
                    $job->sendToWatchers(new BinaryPacket(PacketMagic::RES, $packet->getType(), $packet->getArgumentList()));
                    $worker->assignJob(null);
                    $this->jobQueue->setComplete($job);
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

        $worker = $this->workerManager->getWorker($connection);
        $this->logger->debug('Worker function list', [
            'functions' => $worker->getAvailableFunctions()
        ]);
        $job = $this->jobQueue->dequeue($worker->getAvailableFunctions());
        if (!$job){
            $this->logger->info('No job available for worker.');
            $connection->writePacket(new BinaryPacket(PacketMagic::RES, PacketType::NO_JOB));
        } else {
            $job->running = true;
            $this->logger->info('Assigning job to worker', [
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
            $this->jobQueue->setRunning($job);
        }
    }
}
