<?php

namespace Kicken\Gearman\Events;

use Kicken\Gearman\Network\Endpoint;
use Symfony\Contracts\EventDispatcher\Event;

class ClientConnected extends Event {
    public Endpoint $client;

    public function __construct(Endpoint $client){
        $this->client = $client;
    }
}
