<?php

namespace Kicken\Gearman\Job\Data;

use Kicken\Gearman\Network\Connection;

class ServerJobData extends JobData {
    private string $function;
    private string $uniqueId;
    private string $workload;
    private int $priority;
    private bool $background;
    private array $watchList = [];

    public function __construct(?string $jobHandle, string $function, string $uniqueId, string $workload, int $priority, bool $background){
        parent::__construct($jobHandle);
        $this->function = $function;
        $this->uniqueId = $uniqueId;
        $this->workload = $workload;
        $this->priority = $priority;
        $this->background = $background;
    }

    public function addWatcher(Connection $connection){
        $this->watchList[] = $connection;
    }
}
