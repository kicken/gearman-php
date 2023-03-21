<?php

namespace Kicken\Gearman\Worker;

class WorkerFunction {
    public string $name;
    public ?int $timeout = null;
    public $callback;

    public function __construct(string $name, callable $callback, ?int $timeout = null){
        $this->name = $name;
        $this->timeout = $timeout;
        $this->callback = $callback;
    }
}
