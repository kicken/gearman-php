<?php

namespace Kicken\Gearman\Network;

interface Endpoint {
    public function listen(callable $handler);
}