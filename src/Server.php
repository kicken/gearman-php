<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\GearmanEndpoint;
use Kicken\Gearman\Network\PacketHandler\VersionHandler;
use Kicken\Gearman\Network\ServerStream;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class Server {
    private array $endpointList;
    private LoopInterface $loop;

    public function __construct($endpointList = '127.0.0.1:4730', LoopInterface $loop = null){
        if (!is_array($endpointList)){
            $endpointList = [$endpointList];
        }

        $this->loop = $loop ?? Loop::get();
        $this->endpointList = array_map(function($endpoint){
            if ($endpoint instanceof Endpoint){
                return $endpoint;
            } else if (is_string($endpoint)){
                return new GearmanEndpoint($endpoint, $this->loop);
            } else {
                throw new \InvalidArgumentException();
            }
        }, $endpointList);
    }

    public function run(){
        foreach ($this->endpointList as $endpoint){
            $endpoint->listen(function(ServerStream $stream){
                $stream->addPacketHandler(new VersionHandler());
            });
        }

        $this->loop->run();
    }
}
