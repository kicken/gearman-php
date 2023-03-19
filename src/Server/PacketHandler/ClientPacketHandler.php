<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\JobQueue;
use Kicken\Gearman\Server\ServerJobData;
use Psr\Log\LoggerInterface;

class ClientPacketHandler extends BinaryPacketHandler {
    private JobQueue $jobQueue;
    private LoggerInterface $logger;

    public function __construct(JobQueue $jobQueue, LoggerInterface $logger){
        $this->jobQueue = $jobQueue;
        $this->logger = $logger;
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
            case PacketType::GET_STATUS:
                $this->jobStatus($connection, $packet->getArgument(0));
                break;
            case PacketType::ECHO_REQ:
                $this->handlePing($connection, $packet->getArgumentList());
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
        $this->logger->info('Processing create job command.', [
            'priority' => $priority
            , 'background' => $background
            , 'function' => $function
            , 'uniqueId' => $uniqueId
            , 'handle' => $newHandle
        ]);

        $job = new ServerJobData($newHandle, $function, $uniqueId, $workload, $priority, $background);
        $this->jobQueue->enqueue($job);
        $connection->writePacket(new BinaryPacket(PacketMagic::RES, PacketType::JOB_CREATED, [$newHandle]));
        if (!$background){
            $job->addWatcher($connection);
        }
    }

    private function jobStatus(Connection $connection, string $handle) : void{
        $this->logger->info('Processing job status command', ['handle' => $handle]);
        $job = $this->jobQueue->lookupJob($handle);
        if (!$job){
            $packet = new BinaryPacket(PacketMagic::RES, PacketType::STATUS_RES, [
                $handle
                , 0
                , 0
                , 0
                , 0
            ]);
        } else {
            $packet = new BinaryPacket(PacketMagic::RES, PacketType::STATUS_RES, [
                $job->jobHandle
                , 1
                , (int)$job->running
                , $job->numerator
                , $job->denominator
            ]);
        }

        $connection->writePacket($packet);
    }

    private function handlePing(Connection $connection, array $argumentList) : void{
        $this->logger->info('Processing ping command');
        $packet = new BinaryPacket(PacketMagic::RES, PacketType::ECHO_RES, $argumentList);
        $connection->writePacket($packet);
    }

    private function newHandle() : string{
        static $counter = 0;

        return 'H:' . ++$counter;
    }
}
