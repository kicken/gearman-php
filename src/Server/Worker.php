<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Events\FunctionRegistered;
use Kicken\Gearman\Events\FunctionUnregistered;
use Kicken\Gearman\Events\JobStarted;
use Kicken\Gearman\Events\JobStopped;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\ServiceContainer;
use function Kicken\Gearman\normalizeFunctionName;

class Worker {
    private Endpoint $connection;
    private ServiceContainer $services;
    private array $functionList = [];
    private bool $sleeping = false;
    private ?ServerJobData $currentAssignment = null;

    public function __construct(Endpoint $connection, ServiceContainer $container){
        $this->connection = $connection;
        $this->services = $container;
    }

    public function getConnection() : Endpoint{
        return $this->connection;
    }

    public function getAvailableFunctions() : array{
        return array_keys($this->functionList);
    }

    public function registerFunction(string $function, ?int $timeout = null){
        $this->functionList[normalizeFunctionName($function)] = $timeout;
        $this->services->eventDispatcher->dispatch(new FunctionRegistered($function));
    }

    public function unregisterFunction(string $function){
        unset($this->functionList[normalizeFunctionName($function)]);
        $this->services->eventDispatcher->dispatch(new FunctionUnregistered($function));
    }

    public function isSleeping() : bool{
        return $this->sleeping;
    }

    public function sleep(){
        $this->sleeping = true;
    }

    public function wake(){
        $this->connection->writePacket(new BinaryPacket(PacketMagic::RES, PacketType::NOOP));
        $this->sleeping = false;
    }

    public function canDo(string $function) : bool{
        return array_key_exists(normalizeFunctionName($function), $this->functionList);
    }

    public function assignJob(?ServerJobData $jobData){
        if (!$jobData && $this->currentAssignment){
            $this->services->eventDispatcher->dispatch(new JobStopped($this->currentAssignment));
        }

        $this->currentAssignment = $jobData;
        if ($jobData){
            $this->services->eventDispatcher->dispatch(new JobStarted($jobData));
        }
    }

    public function getCurrentJob() : ?ServerJobData{
        return $this->currentAssignment;
    }
}
