<?php

namespace Kicken\Gearman\Events;

final class ServerEvents {
    const WORKER_CONNECTED = __CLASS__ . '::WORKER_CONNECTED';
    const WORKER_DISCONNECTED = __CLASS__ . '::WORKER_DISCONNECTED';
    const JOB_QUEUED = __CLASS__ . '::JOB_QUEUED';
    const JOB_STARTED = __CLASS__ . '::JOB_STARTED';
    const JOB_STOPPED = __CLASS__ . '::JOB_STOPPED';

    private function __construct(){
    }
}
