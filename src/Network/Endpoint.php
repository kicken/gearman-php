<?php

namespace Kicken\Gearman\Network;

use React\Promise\ExtendedPromiseInterface;

interface Endpoint {
    public function connect() : ExtendedPromiseInterface;

    public function getAddress() : string;

    public function listen(callable $handler);

    public function shutdown();
}