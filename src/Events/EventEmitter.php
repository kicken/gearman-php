<?php

namespace Kicken\Gearman\Events;

trait EventEmitter {
    private array $callbackList = [];

    private function emit(string $event, ...$args) : void{
        foreach ($this->callbackList[$event] ?? [] as $callback){
            call_user_func_array($callback, $args);
        }
    }

    public function on(string $event, callable $callback) : void{
        $this->callbackList[$event][] = $callback;
    }
}
