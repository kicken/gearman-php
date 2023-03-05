<?php

namespace Kicken\Gearman\Protocol;

class AdministrativePacket implements Packet {
    private string $data;

    public function __construct(string $command){
        $this->data = $command;
    }

    public function getData() : string{
        return $this->data;
    }

    public function __toString(){
        return $this->data . "\r\n";
    }
}