<?php

namespace Kicken\Gearman\Server;

interface JobQueue {
    public function enqueue(ServerJobData $jobData) : void;

    public function dequeue(array $functionList) : ?ServerJobData;

    public function setRunning(ServerJobData $jobData) : void;

    public function setComplete(ServerJobData $jobData) : void;

    public function findByHandle(string $handle) : ?ServerJobData;

    public function getFunctionList() : array;

    public function getTotalJobs(string $function) : int;

    public function getTotalRunning(string $function) : int;
}
