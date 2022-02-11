<?php

namespace Kicken\Gearman\Job\Data;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Protocol\Packet;

class ServerJobData extends JobData {
    public string $function;
    public string $uniqueId;
    public string $workload;
    public int $priority;
    public bool $background;
    public bool $running = false;
    /** @var Connection[] */
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

    public function getWatcherList() : array{
        return $this->watchList;
    }

    public function sendToWatchers(Packet $packet){
        foreach ($this->watchList as $watcher){
            $watcher->writePacket($packet);
        }
    }
}
