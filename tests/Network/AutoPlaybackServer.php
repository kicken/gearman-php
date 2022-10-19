<?php

namespace Kicken\Gearman\Test\Network;

use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class AutoPlaybackServer extends PacketPlaybackConnection {
    private LoopInterface $loop;

    public function __construct(array $packetSequence = [], LoopInterface $loop = null){
        parent::__construct($packetSequence);
        $this->loop = $loop ?? Loop::get();
        $this->loop->futureTick(function(){
            $this->playback();
        });
    }
}