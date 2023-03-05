<?php

namespace Kicken\Gearman\Protocol;

class AdministrativeCommandPacket extends AdministrativePacket {
    private array $arguments;

    public function __construct(string $data){
        parent::__construct($data);
        $this->arguments = preg_split('/\s+/', $data);
    }

    public function getCommand() : string{
        return strtolower(trim($this->getArgument(0)));
    }

    public function getArgument(int $index) : string{
        return $this->arguments[$index] ?? '';
    }

    public function __toString(){
        return $this->getData() . "\r\n";
    }
}