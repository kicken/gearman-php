<?php

namespace Kicken\Gearman\Events;

class FunctionUnregistered {
    public string $function;

    public function __construct(string $function){
        $this->function = $function;
    }
}
