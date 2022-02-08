<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Protocol\Packet;

class WorkerConnection {
    private Connection $connection;
    private array $canDoList = [];
    private bool $sleeping = false;

    public function __construct(Connection $connection){
        $this->connection = $connection;
    }

    public function registerFunction(string $function, ?int $timeout = null) : void{
        $this->canDoList[$function] = $timeout;
    }

    public function unregisterFunction(string $function) : void{
        unset($this->canDoList[$function]);
    }

    public function getAvailableFunctions() : array{
        return array_keys($this->canDoList);
    }

    public function send(Packet $packet) : void{
        $this->connection->writePacket($packet);
    }

    public function isSleeping(?bool $sleeping = null) : bool{
        if ($sleeping === null){
            return $this->sleeping;
        } else {
            return $this->sleeping = $sleeping;
        }
    }
}
