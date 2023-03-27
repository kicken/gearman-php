<?php

namespace Kicken\Gearman\Worker\PacketHandler;

use Kicken\Gearman\Exception\NoWorkException;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Worker\WorkerJob;
use Kicken\Gearman\Worker\WorkJobData;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;

class GrabJobHandler extends BinaryPacketHandler {
    private Deferred $deferred;
    private LoggerInterface $logger;

    public function __construct(?LoggerInterface $logger = null){
        $this->logger = $logger ?? new NullLogger();
    }

    public function grabJob(Endpoint $server) : ExtendedPromiseInterface{
        $this->logger->debug('Requesting job from server', [
            'server' => $server->getAddress()
        ]);

        $this->deferred = new Deferred();
        $packet = new BinaryPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ);
        $server->writePacket($packet);
        $server->addPacketHandler($this);

        return $this->deferred->promise();
    }

    public function handleBinaryPacket(Endpoint $connection, BinaryPacket $packet) : bool{
        switch ($packet->getType()){
            case PacketType::NO_JOB:
                $this->logger->debug('No job available, going to sleep', [
                    'server' => $connection->getAddress()
                ]);
                $this->deferred->reject(new NoWorkException());
                $connection->removePacketHandler($this);

                return true;
            case PacketType::JOB_ASSIGN:
            case PacketType::JOB_ASSIGN_UNIQ:
                $job = $this->createJob($connection, $packet);
                $this->logger->debug('Job assigned', [
                    'server' => $connection->getAddress()
                    , 'handle' => $job->getJobHandle()
                ]);
                $this->deferred->resolve($job);
                $connection->removePacketHandler($this);

                return true;
            default:
                return false;
        }
    }

    private function createJob(Endpoint $server, BinaryPacket $packet) : WorkerJob{
        if ($packet->getType() === PacketType::JOB_ASSIGN){
            $details = new WorkJobData($packet->getArgument(0), $packet->getArgument(1), null, JobPriority::NORMAL, $packet->getArgument(2));
        } else {
            $details = new WorkJobData($packet->getArgument(0), $packet->getArgument(1), $packet->getArgument(2), JobPriority::NORMAL, $packet->getArgument(3));
        }

        return new WorkerJob($server, $details);
    }
}