<?php

namespace Kicken\Gearman\Server;

class Worker {
    private array $functionList = [];
    private bool $sleeping = false;

    public function getAvailableFunctions() : array{
        return array_keys($this->functionList);
    }

    public function registerFunction(string $function, ?int $timeout){
        $this->functionList[$function] = $timeout;
    }

    public function unregisterFunction(string $function){
        unset($this->functionList[$function]);
    }

    public function canDo(string $function) : bool{
        return isset($this->functionList[$function]);
    }

    public function sleep(){
        $this->sleeping = true;
    }

    public function wake(){
        $this->sleeping = false;
    }

    public function isSleeping() : bool{
        return $this->sleeping;
    }
}
