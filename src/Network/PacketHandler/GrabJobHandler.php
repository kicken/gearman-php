<?php

namespace Kicken\Gearman\Network\PacketHandler;

use Kicken\Gearman\Job\Data\WorkJobData;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Job\WorkerJob;
use Kicken\Gearman\Network\Server;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use React\Promise\Deferred;

class GrabJobHandler implements PacketHandler {
    private Deferred $deferred;

    public function __construct(){
        $this->deferred = new Deferred();
    }

    public function grabJob(Server $server){
        $this->issueGrabJob($server);
        $server->addPacketHandler($this);

        return $this->deferred->promise();
    }

    public function handlePacket(Server $server, Packet $packet) : bool{
        switch ($packet->getType()){
            case PacketType::NO_JOB:
                $this->sleep($server);

                return true;
            case PacketType::JOB_ASSIGN:
            case PacketType::JOB_ASSIGN_UNIQ:
                $this->deferred->resolve($this->createJob($server, $packet));
                $server->removePacketHandler($this);

                return true;
            case PacketType::NOOP:
                $this->issueGrabJob($server);

                return true;
            default:
                return false;
        }
    }

    private function sleep(Server $server) : void{
        $packet = new Packet(PacketMagic::REQ, PacketType::PRE_SLEEP);
        $server->writePacket($packet);
    }

    private function issueGrabJob(Server $server){
        $packet = new Packet(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ);
        $server->writePacket($packet);
    }

    private function createJob(Server $server, Packet $packet) : WorkerJob{
        if ($packet->getType() === PacketType::JOB_ASSIGN){
            $details = new WorkJobData($packet->getArgument(0), $packet->getArgument(1), null, JobPriority::NORMAL, $packet->getArgument(2));
        } else {
            $details = new WorkJobData($packet->getArgument(0), $packet->getArgument(1), $packet->getArgument(2), JobPriority::NORMAL, $packet->getArgument(3));
        }

        return new WorkerJob($server, $details);
    }
}