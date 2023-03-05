<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Events\EventEmitter;
use Kicken\Gearman\Events\ServerEvents;
use Kicken\Gearman\Job\Data\ServerJobData;
use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;

class Worker {
    use EventEmitter;

    private Connection $connection;
    private array $functionList = [];
    private bool $sleeping = false;
    private string $clientId;
    private ?ServerJobData $currentAssignment = null;

    public function __construct(Connection $connection){
        $this->connection = $connection;
        $this->clientId = uniqid();
    }

    public function getConnection() : Connection{
        return $this->connection;
    }

    public function getClientId() : string{
        return $this->clientId;
    }

    public function getAvailableFunctions() : array{
        return array_keys($this->functionList);
    }

    public function registerFunction(string $function, ?int $timeout = null){
        $this->functionList[$function] = $timeout;
        $this->emit(ServerEvents::WORKER_REGISTERED_FUNCTION, $this, $function);
    }

    public function unregisterFunction(string $function){
        unset($this->functionList[$function]);
        $this->emit(ServerEvents::WORKER_UNREGISTERED_FUNCTION, $this, $function);
    }

    public function sleep(){
        $this->sleeping = true;
    }

    public function wake(){
        if ($this->sleeping){
            $this->connection->writePacket(new BinaryPacket(PacketMagic::RES, PacketType::NOOP));
        }

        $this->sleeping = false;
    }

    public function canDo(ServerJobData $jobData) : bool{
        if ($this->currentAssignment){
            return false;
        }

        if (!array_key_exists($jobData->function, $this->functionList)){
            return false;
        }

        return true;
    }

    public function assignJob(?ServerJobData $jobData){
        $this->currentAssignment = $jobData;
    }

    public function getCurrentJob() : ?ServerJobData{
        return $this->currentAssignment;
    }
}
