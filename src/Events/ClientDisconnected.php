<?php

namespace Kicken\Gearman\Events;

use Kicken\Gearman\Network\Endpoint;

class ClientDisconnected {
    public Endpoint $client;

    public function __construct(Endpoint $client){
        $this->client = $client;
    }
}
