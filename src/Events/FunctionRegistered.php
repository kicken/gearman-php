<?php

namespace Kicken\Gearman\Events;

use Symfony\Contracts\EventDispatcher\Event;

class FunctionRegistered extends Event {
    public string $function;

    public function __construct(string $function){
        $this->function = $function;
    }
}
