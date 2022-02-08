<?php

namespace Kicken\Gearman\Test\Network;

use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Promise\ExtendedPromiseInterface;

class AutoPlaybackServer extends PacketPlaybackConnection {
    private LoopInterface $loop;

    public function __construct(array $packetSequence = [], LoopInterface $loop = null){
        parent::__construct($packetSequence);
        $this->loop = $loop ?? Loop::get();
    }

    public function connect() : ExtendedPromiseInterface{
        /** @noinspection PhpIncompatibleReturnTypeInspection */
        return parent::connect()->then(function(){
            $this->loop->futureTick(function(){
                $this->playback();
            });

            return $this;
        });
    }
}