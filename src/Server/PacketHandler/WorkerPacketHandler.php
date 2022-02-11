<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\JobQueue;
use Kicken\Gearman\Server\WorkerManager;

class WorkerPacketHandler extends BinaryPacketHandler {
    private WorkerManager $workerManager;
    private JobQueue $jobQueue;

    public function __construct(WorkerManager $manager, JobQueue $jobQueue){
        $this->workerManager = $manager;
        $this->jobQueue = $jobQueue;
    }

    public function handleBinaryPacket(Connection $connection, BinaryPacket $packet) : bool{
        switch ($packet->getType()){
            case PacketType::CAN_DO:
                $this->workerManager->getWorker($connection)->registerFunction($packet->getArgument(0));
                break;
            case PacketType::CAN_DO_TIMEOUT:
                $this->workerManager->getWorker($connection)->registerFunction($packet->getArgument(0), $packet->getArgument(1));
                break;
            case PacketType::PRE_SLEEP:
                $this->workerManager->getWorker($connection)->sleep();
                break;
            case PacketType::GRAB_JOB:
            case PacketType::GRAB_JOB_UNIQ:
                $this->assignJob($connection, $packet->getType());
                break;
            case PacketType::WORK_DATA:
            case PacketType::WORK_WARNING:
                $job = $this->workerManager->getWorker($connection)->getCurrentJob();
                $job->sendToWatchers(new BinaryPacket(PacketMagic::RES, $packet->getType(), $packet->getArgumentList()));
                break;
            case PacketType::WORK_STATUS:
                $job = $this->workerManager->getWorker($connection)->getCurrentJob();
                $job->numerator = $packet->getArgument(1);
                $job->denominator = $packet->getArgument(2);
                $job->sendToWatchers(new BinaryPacket(PacketMagic::RES, $packet->getType(), $packet->getArgumentList()));
                break;
            case PacketType::WORK_FAIL:
            case PacketType::WORK_COMPLETE:
            case PacketType::WORK_EXCEPTION:
                $worker = $this->workerManager->getWorker($connection);
                $job = $worker->getCurrentJob();
                $job->sendToWatchers(new BinaryPacket(PacketMagic::RES, $packet->getType(), $packet->getArgumentList()));
                $worker->assignJob(null);
                $this->jobQueue->deleteJob($job);
                break;
            default:
                return false;
        }

        return true;
    }

    private function assignJob(Connection $connection, int $grabType){
        $worker = $this->workerManager->getWorker($connection);
        $job = $this->jobQueue->findJob($worker);
        $worker->assignJob($job);

        if (!$job){
            $connection->writePacket(new BinaryPacket(PacketMagic::RES, PacketType::NO_JOB));
        } else {
            $job->running = true;
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
