<?php

namespace Kicken\Gearman\Network;

use React\Promise\PromiseInterface;

interface Endpoint {
    public function connect(bool $autoDisconnect) : PromiseInterface;

    public function disconnect() : void;

    public function getAddress() : string;

    public function listen(callable $handler);

    public function shutdown();
}