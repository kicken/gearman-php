<?php

namespace Kicken\Gearman\Test;

use Kicken\Gearman\DefaultEventDispatcher;

class MockDispatcher extends DefaultEventDispatcher {
    private array $events = [];

    public function wasDispatched(string $class) : bool{
        foreach ($this->events as $event){
            if ($event instanceof $class){
                return true;
            }
        }

        return false;
    }

    public function dispatch(object $event){
        parent::dispatch($event);
        $this->events[] = $event;
    }
}
