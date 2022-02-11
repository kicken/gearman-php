<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Job\Data\ServerJobData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\JobQueue;

class ClientPacketHandler extends BinaryPacketHandler {
    private JobQueue $jobQueue;

    public function __construct(JobQueue $jobQueue){
        $this->jobQueue = $jobQueue;
    }

    public function handleBinaryPacket(Connection $connection, BinaryPacket $packet) : bool{
        switch ($packet->getType()){
            case PacketType::SUBMIT_JOB:
            case PacketType::SUBMIT_JOB_BG:
                $this->createJob($connection, $packet, JobPriority::NORMAL, $packet->getType() === PacketType::SUBMIT_JOB_BG);
                break;
            case PacketType::SUBMIT_JOB_LOW:
            case PacketType::SUBMIT_JOB_LOW_BG:
                $this->createJob($connection, $packet, JobPriority::LOW, $packet->getType() === PacketType::SUBMIT_JOB_LOW_BG);
                break;
            case PacketType::SUBMIT_JOB_HIGH:
            case PacketType::SUBMIT_JOB_HIGH_BG:
                $this->createJob($connection, $packet, JobPriority::HIGH, $packet->getType() === PacketType::SUBMIT_JOB_HIGH_BG);
                break;
            default:
                return false;
        }

        return true;
    }

    private function createJob(Connection $connection, BinaryPacket $packet, int $priority, bool $background) : void{
        $newHandle = $this->newHandle();
        $function = $packet->getArgument(0);
        $uniqueId = $packet->getArgument(1);
        $workload = $packet->getArgument(2);

        $job = new ServerJobData($newHandle, $function, $uniqueId, $workload, $priority, $background);
        $this->jobQueue->enqueue($job);
        $connection->writePacket(new BinaryPacket(PacketMagic::RES, PacketType::JOB_CREATED, [$newHandle]));
        if (!$background){
            $job->addWatcher($connection);
        }
    }

    private function newHandle() : string{
        static $counter = 0;

        return 'H:' . ++$counter;
    }
}
