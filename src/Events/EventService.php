<?php

namespace Kicken\Gearman\Events;

use Psr\EventDispatcher\EventDispatcherInterface;

interface EventService extends EventDispatcherInterface {
    public function addListener(string $event, callable $listener) : void;
}
