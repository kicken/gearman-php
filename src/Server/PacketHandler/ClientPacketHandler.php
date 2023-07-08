<?php

namespace Kicken\Gearman\Server\PacketHandler;

use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Server\JobQueue\JobQueue;
use Kicken\Gearman\Server\ServerJobData;
use Kicken\Gearman\Server\WorkerManager;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ClientPacketHandler extends BinaryPacketHandler {
    private JobQueue $jobQueue;
    private WorkerManager $workerManager;
    private LoggerInterface $logger;
    private string $handlePrefix;
    private static int $handleCounter = 0;

    public function __construct(string $handlePrefix, JobQueue $jobQueue, WorkerManager $workerManager, ?LoggerInterface $logger = null){
        $this->jobQueue = $jobQueue;
        $this->workerManager = $workerManager;
        $this->logger = $logger ?? new NullLogger();
        $this->handlePrefix = $handlePrefix;
    }

    public function handleBinaryPacket(Endpoint $connection, BinaryPacket $packet) : bool{
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

    private function createJob(Endpoint $connection, BinaryPacket $packet, int $priority, bool $background) : void{
        $handle = $this->newHandle();
        $function = $packet->getArgument(0);
        $uniqueId = $packet->getArgument(1);
        $workload = $packet->getArgument(2);

        $job = new ServerJobData($handle, $function, $uniqueId, $workload, $priority, $background, new \DateTimeImmutable());
        $this->jobQueue->enqueue($job);
        $connection->writePacket(new BinaryPacket(PacketMagic::RES, PacketType::JOB_CREATED, [$job->jobHandle]));
        if (!$background){
            $job->addWatcher($connection);
        }
        $this->logger->info('Processed create job command.', [
            'priority' => $priority
            , 'background' => $background
            , 'function' => $function
            , 'uniqueId' => $uniqueId
            , 'handle' => $job->jobHandle
        ]);
        $this->workerManager->wakeAllCandidates($job);
    }

    private function jobStatus(Endpoint $connection, string $handle) : void{
        $this->logger->info('Processing job status command', ['handle' => $handle]);
        $job = $this->jobQueue->findByHandle($handle);
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

    private function handlePing(Endpoint $connection, array $argumentList) : void{
        $this->logger->info('Processing ping command');
        $packet = new BinaryPacket(PacketMagic::RES, PacketType::ECHO_RES, $argumentList);
        $connection->writePacket($packet);
    }

    private function newHandle() : string{
        return $this->handlePrefix . ':' . (++self::$handleCounter);
    }
}
