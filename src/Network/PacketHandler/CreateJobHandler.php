<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Job\Data\ClientJobData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Network\Server;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;

class CreateJobHandler implements PacketHandler {
    private ClientJobData $data;
    private Deferred $jobHandleDeferred;

    public function __construct(ClientJobData $data){
        $this->data = $data;
        $this->jobHandleDeferred = new Deferred();
    }

    public function handlePacket(Server $server, Packet $packet) : bool{
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

    public function createJob(Server $server) : PromiseInterface{
        $packetType = $this->getSubmitJobType($this->data->priority, $this->data->background);
        $arguments = [$this->data->function, $this->data->unique, $this->data->workload];

        $packet = new Packet(PacketMagic::REQ, $packetType, $arguments);
        $server->writePacket($packet);
        $server->addPacketHandler($this);

        return $this->jobHandleDeferred->promise();
    }

    private function updateJobData(Server $server, Packet $packet){
        switch ($packet->getType()){
            case PacketType::JOB_CREATED:
                $this->data->jobHandle = $packet->getArgument(0);
                if ($this->data->background){
                    $server->removePacketHandler($this);
                }
                break;
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
            case PacketType::WORK_EXCEPTION:
                $this->data->data = $packet->getArgument(1);
                $this->data->result = $this->data->data;
                $this->data->finished = true;
                $this->data->triggerCallback($packet->getType() == PacketType::WORK_COMPLETE ? 'complete' : 'warning');
                $server->removePacketHandler($this);
                break;
            case PacketType::WORK_FAIL:
                $this->data->finished = true;
                $this->data->triggerCallback('fail');
                $server->removePacketHandler($this);
                break;
            case PacketType::WORK_DATA:
                $this->data->data = $packet->getArgument(1);
                $this->data->triggerCallback('data');
                break;
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
