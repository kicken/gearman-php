<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Job\Data\JobData;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\Packet;

class ServerJobData extends JobData {
    public string $function;
    public string $uniqueId;
    public string $reducer;
    public string $workload;
    public \DateTimeImmutable $created;
    public int $priority;
    public bool $background;
    public bool $running = false;
    /** @var Endpoint[] */
    private array $watchList = [];

    public function __construct(string $jobHandle, string $function, string $uniqueId, string $workload, int $priority, bool $background, \DateTimeImmutable $created){
        parent::__construct($jobHandle);
        $this->function = $function;
        $this->uniqueId = $uniqueId;
        $this->reducer = '';
        $this->workload = $workload;
        $this->priority = $priority;
        $this->background = $background;
        $this->created = $created;
    }

    public function addWatcher(Endpoint $connection){
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
