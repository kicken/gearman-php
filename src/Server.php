<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\VersionHandler;
use Kicken\Gearman\Server\PacketHandler\WorkerJobHandler;
use Kicken\Gearman\Server\PacketHandler\WorkerRegistrationHandler;
use Kicken\Gearman\Server\WorkerRegistry;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class Server {
    private array $endpointList;
    private LoopInterface $loop;
    private WorkerRegistry $workerRegistry;

    public function __construct($endpointList = '127.0.0.1:4730', LoopInterface $loop = null){
        if (!is_array($endpointList)){
            $endpointList = [$endpointList];
        }

        $this->loop = $loop ?? Loop::get();
        $this->endpointList = mapToEndpointObjects($endpointList, $this->loop);
        $this->workerRegistry = new WorkerRegistry();
    }

    public function run(){
        foreach ($this->endpointList as $endpoint){
            $endpoint->listen(function(Connection $stream){
                $stream->addPacketHandler(new WorkerRegistrationHandler($this->workerRegistry));
                $stream->addPacketHandler(new WorkerJobHandler($this->workerRegistry));
                $stream->addPacketHandler(new VersionHandler());
            });
        }

        $this->loop->run();
    }
}
