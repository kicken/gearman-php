<?php

namespace Kicken\Gearman\Client\PacketHandler;

use Kicken\Gearman\Client\ClientJobData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\BinaryPacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;

class CreateJobHandler extends BinaryPacketHandler {
    private ClientJobData $data;
    private Deferred $jobHandleDeferred;
    private LoggerInterface $logger;

    public function __construct(ClientJobData $data, ?LoggerInterface $logger = null){
        $this->data = $data;
        $this->jobHandleDeferred = new Deferred();
        $this->logger = $logger ?? new NullLogger();
    }

    public function handleBinaryPacket(Endpoint $connection, BinaryPacket $packet) : bool{
        if ($packet->getType() === PacketType::JOB_CREATED && !$this->data->jobHandle){
            $this->data->jobHandle = $packet->getArgument(0);
            $this->jobHandleDeferred->resolve();
            if ($this->data->background){
                $connection->removePacketHandler($this);
            }

            $this->logger->debug('Job created.', [
                'server' => $connection->getAddress()
                , 'function' => $this->data->function
                , 'handle' => $this->data->jobHandle
            ]);

            return true;
        } else if ($packet->getArgument(0) === $this->data->jobHandle){
            $this->logger->debug('Received job details update.', [
                'server' => $connection->getAddress()
                , 'function' => $this->data->function
                , 'handle' => $this->data->jobHandle
            ]);
            $this->updateJobData($connection, $packet);

            return true;
        }

        return false;
    }

    public function createJob(Endpoint $server) : ExtendedPromiseInterface{
        $packetType = $this->getSubmitJobType($this->data->priority, $this->data->background);
        $arguments = [$this->data->function, $this->data->unique, $this->data->workload];

        $packet = new BinaryPacket(PacketMagic::REQ, $packetType, $arguments);
        $server->writePacket($packet);
        $server->addPacketHandler($this);

        $this->logger->debug('Sending create job to server', ['server' => $server->getAddress(), 'function' => $this->data->function]);

        return $this->jobHandleDeferred->promise();
    }

    private function updateJobData(Endpoint $server, BinaryPacket $packet){
        switch ($packet->getType()){
            case PacketType::WORK_STATUS:
                $this->data->numerator = (int)$packet->getArgument(1);
                $this->data->denominator = (int)$packet->getArgument(2);
                $this->data->triggerCallback('status');
                break;
            case PacketType::WORK_WARNING:
                $this->data->data = $packet->getArgument(1);
                $this->data->triggerCallback('warning');
                break;
            case PacketType::WORK_COMPLETE:
                $this->data->result = $packet->getArgument(1);
                $this->data->finished = true;
                $this->data->triggerCallback('complete');
                break;
            case PacketType::WORK_EXCEPTION:
                $this->data->data = $packet->getArgument(1);
                $this->data->finished = true;
                $this->data->triggerCallback('exception');
                break;
            case PacketType::WORK_FAIL:
                $this->data->finished = true;
                $this->data->triggerCallback('fail');
                break;
            case PacketType::WORK_DATA:
                $this->data->data = $packet->getArgument(1);
                $this->data->triggerCallback('data');
                break;
        }

        if ($this->data->finished){
            $server->removePacketHandler($this);
        }
    }

    private function getSubmitJobType(int $priority, bool $background) : int{
        switch ($priority){
            case JobPriority::HIGH:
                return $background ? PacketType::SUBMIT_JOB_HIGH_BG : PacketType::SUBMIT_JOB_HIGH;
            case JobPriority::NORMAL:
                return $background ? PacketType::SUBMIT_JOB_BG : PacketType::SUBMIT_JOB;
            case JobPriority::LOW:
                return $background ? PacketType::SUBMIT_JOB_LOW_BG : PacketType::SUBMIT_JOB_LOW;
            default:
                throw new \InvalidArgumentException('Invalid job priority');
        }
    }
}
