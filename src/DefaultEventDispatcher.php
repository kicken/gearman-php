<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Events\EventService;
use Symfony\Component\EventDispatcher\EventDispatcher;

class DefaultEventDispatcher implements EventService {
    private EventDispatcher $dispatcher;

    public function __construct(){
        $this->dispatcher = new EventDispatcher();
    }

    public function addListener(string $event, callable $listener) : void{
        $this->dispatcher->addListener($event, $listener);
    }

    public function dispatch(object $event){
        $this->dispatcher->dispatch($event);
    }
}
