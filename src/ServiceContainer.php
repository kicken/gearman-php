<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Server\JobQueue\JobQueue;
use Kicken\Gearman\Server\JobQueue\MemoryJobQueue;
use Kicken\Gearman\Server\Statistics;
use Kicken\Gearman\Server\WorkerManager;
use Kicken\Gearman\Worker\FunctionRegistry;
use Psr\EventDispatcher\EventDispatcherInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use Symfony\Component\EventDispatcher\EventDispatcher;

class ServiceContainer {
    public LoggerInterface $logger;
    public EventDispatcherInterface $eventDispatcher;
    public LoopInterface $loop;
    public WorkerManager $workerManager;
    public JobQueue $jobQueue;
    public Statistics $statistics;
    public FunctionRegistry $functionRegistry;

    public function __construct(){
        $this->logger = new NullLogger();
        $this->loop = Loop::get();
        $this->eventDispatcher = new EventDispatcher();
        $this->workerManager = new WorkerManager($this);
        $this->jobQueue = new MemoryJobQueue();
        $this->statistics = new Statistics($this);
        $this->functionRegistry = new FunctionRegistry($this);
    }
}
