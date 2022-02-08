<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Job\Data\ClientJobData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Network\Server;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;

class CreateJobHandler extends BinaryPacketHandler {
    private ClientJobData $data;
    private Deferred $jobHandleDeferred;

    public function __construct(ClientJobData $data){
        $this->data = $data;
        $this->jobHandleDeferred = new Deferred();
    }

    public function handleBinaryPacket(Server $server, BinaryPacket $packet) : bool{
        if ($packet->getType() === PacketType::JOB_CREATED && !$this->data->jobHandle){
            $this->data->jobHandle = $packet->getArgument(0);
            $this->jobHandleDeferred->resolve();
            if ($this->data->background){
                $server->removePacketHandler($this);
            }

            return true;
        } else if ($packet->getArgument(0) === $this->data->jobHandle){
            $this->updateJobData($server, $packet);

            return true;
        }

        return false;
    }

    public function createJob(Server $server) : ExtendedPromiseInterface{
        $packetType = $this->getSubmitJobType($this->data->priority, $this->data->background);
        $arguments = [$this->data->function, $this->data->unique, $this->data->workload];

        $packet = new BinaryPacket(PacketMagic::REQ, $packetType, $arguments);
        $server->writePacket($packet);
        $server->addPacketHandler($this);

        return $this->jobHandleDeferred->promise();
    }

    private function updateJobData(Server $server, BinaryPacket $packet){
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
