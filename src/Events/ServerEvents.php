<?php

namespace Kicken\Gearman\Events;

final class ServerEvents {
    const WORKER_CONNECTED = __CLASS__ . '::WORKER_CONNECTED';
    const WORKER_DISCONNECTED = __CLASS__ . '::WORKER_DISCONNECTED';
    const WORKER_REGISTERED_FUNCTION = __CLASS__ . '::WORKER_REGISTERED_FUNCTION';
    const WORKER_UNREGISTERED_FUNCTION = __CLASS__ . '::WORKER_UNREGISTERED_FUNCTION';
    const JOB_QUEUED = __CLASS__ . '::JOB_QUEUED';
    const JOB_STARTED = __CLASS__ . '::JOB_STARTED';
    const JOB_STOPPED = __CLASS__ . '::JOB_STOPPED';
    const JOB_REMOVED = __CLASS__ . '::JOB_REMOVED';

    private function __construct(){
    }
}
