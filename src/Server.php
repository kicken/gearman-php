<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Server\PacketHandler\AdminPacketHandler;
use Kicken\Gearman\Server\PacketHandler\ClientPacketHandler;
use Kicken\Gearman\Server\PacketHandler\WorkerPacketHandler;
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
                $stream->addPacketHandler(new AdminPacketHandler($this->workerRegistry));
                $stream->addPacketHandler(new ClientPacketHandler());
                $stream->addPacketHandler(new WorkerPacketHandler($this->workerRegistry));
            });
        }

        $this->loop->run();
    }
}
