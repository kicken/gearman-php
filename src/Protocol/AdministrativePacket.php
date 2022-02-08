<?php

namespace Kicken\Gearman\Protocol;

class AdministrativePacket implements Packet {
    private string $command;

    public function __construct(string $command){
        $this->command = strtolower(trim($command));
    }

    public function getCommand() : string{
        return $this->command;
    }
}