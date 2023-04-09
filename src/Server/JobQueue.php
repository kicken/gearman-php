<?php

namespace Kicken\Gearman\Server;

interface JobQueue {
    public function enqueue(ServerJobData $jobData) : void;

    public function dequeue(array $functionList) : ?ServerJobData;

    public function findByHandle(string $handle) : ?ServerJobData;
}
