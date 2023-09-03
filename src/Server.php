<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Events\EndpointEvents;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Server\JobQueue\JobQueue;
use Kicken\Gearman\Server\JobQueue\MemoryJobQueue;
use Kicken\Gearman\Server\PacketHandler\AdminPacketHandler;
use Kicken\Gearman\Server\PacketHandler\ClientIdPacketHandler;
use Kicken\Gearman\Server\PacketHandler\ClientPacketHandler;
use Kicken\Gearman\Server\PacketHandler\OptionsPacketHandler;
use Kicken\Gearman\Server\PacketHandler\WorkerPacketHandler;
use Kicken\Gearman\Server\Statistics;
use Kicken\Gearman\Server\WorkerManager;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class Server implements LoggerAwareInterface {
    /** @var Endpoint[] */
    private array $endpointList;
    private LoopInterface $loop;
    private WorkerManager $workerRegistry;
    private JobQueue $jobQueue;
    private Statistics $statistics;
    private bool $isShutdown = false;
    private string $handlePrefix;
    private LoggerInterface $logger;

    public function __construct($endpointList = '127.0.0.1:4730', string $handlePrefix = null, JobQueue $queue = null, LoopInterface $loop = null){
        if (!is_array($endpointList)){
            $endpointList = [$endpointList];
        }

        $this->logger = new NullLogger();
        $this->loop = $loop ?? Loop::get();
        $this->endpointList = mapToEndpointObjects($endpointList, $this->loop);
        $this->workerRegistry = new WorkerManager();
        $this->jobQueue = $queue ?? new MemoryJobQueue();
        $this->statistics = new Statistics($this->workerRegistry, $this->jobQueue, $this->logger);
        $this->handlePrefix = $handlePrefix ?? 'H:' . bin2hex(random_bytes(4));
    }

    /**
     * Determine if the server has been shutdown.
     *
     * @return bool
     */
    public function isShutdown() : bool{
        return $this->isShutdown;
    }

    /**
     * Set a logger.
     *
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger) : void{
        $this->logger = $logger;
        $this->statistics->setLogger($logger);
        foreach ($this->endpointList as $item){
            if ($item instanceof LoggerAwareInterface){
                $item->setLogger($this->logger);
            }
        }
    }

    /**
     * Start listening on the configured endpoints for client connections.
     *
     * @return void
     */
    public function run() : void{
        foreach ($this->endpointList as $endpoint){
            $this->logger->info('Listening on ' . $endpoint->getAddress());
            $endpoint->listen(function(Endpoint $stream){
                $this->logger->info('Received connection from ' . $stream->getAddress());
                $stream->addPacketHandler(new AdminPacketHandler($this, $this->statistics, $this->logger));
                $stream->addPacketHandler(new ClientIdPacketHandler());
                $stream->addPacketHandler(new OptionsPacketHandler());
                $stream->addPacketHandler(new ClientPacketHandler($this->handlePrefix, $this->jobQueue, $this->workerRegistry, $this->logger));
                $stream->addPacketHandler(new WorkerPacketHandler($this, $this->workerRegistry, $this->jobQueue, $this->logger));
                $stream->on(EndpointEvents::DISCONNECTED, function(Endpoint $connection){
                    $this->logger->info('Lost connection to ' . $connection->getAddress());
                    $worker = $this->workerRegistry->getWorker($connection);
                    $job = $worker->getCurrentJob();
                    if ($job){
                        $this->logger->notice('Re-queuing active job', ['handle' => $job->jobHandle]);
                        $this->jobQueue->enqueue($job);
                    }
                    $this->workerRegistry->removeConnection($connection);
                });
            });
        }

        $this->loop->run();
    }

    /**
     * Stop accepting new client connections and shutdown the server.
     *
     * @param bool $graceful Let current worker clients continue their jobs.
     *
     * @return void
     */
    public function shutdown(bool $graceful = true) : void{
        $this->logger->notice('Server shutdown requested.', ['graceful' => $graceful]);
        $this->isShutdown = true;
        foreach ($this->endpointList as $endpoint){
            $endpoint->shutdown();
        }

        if (!$graceful){
            $this->workerRegistry->disconnectAll();
        } else {
            $this->workerRegistry->disconnectSleeping();
        }
    }
}
