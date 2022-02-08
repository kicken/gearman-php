<?php

namespace Kicken\Gearman\Exception;

use Kicken\Gearman\Protocol\Packet;

class UnhandledPacket extends \RuntimeException {
    private Packet $packet;

    public function __construct(Packet $packet){
        parent::__construct('Unhandled packet');
        $this->packet = $packet;
    }

    public function getPacket() : Packet{
        return $this->packet;
    }
}