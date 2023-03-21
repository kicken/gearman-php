<?php

namespace Kicken\Gearman\Events;

final class WorkerEvents {
    const REGISTERED_FUNCTION = __CLASS__ . '::REGISTERED_FUNCTION';
    const UNREGISTERED_FUNCTION = __CLASS__ . '::UNREGISTERED_FUNCTION';

    private function __construct(){
    }
}