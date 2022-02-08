<?php

namespace Kicken\Gearman\Network;

use React\Promise\ExtendedPromiseInterface;

interface Endpoint {
    public function connect() : ExtendedPromiseInterface;

    public function listen(callable $handler);
}