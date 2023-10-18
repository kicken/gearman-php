<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Events\ClientDisconnected;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Server\PacketHandler\AdminPacketHandler;
use Kicken\Gearman\Server\PacketHandler\ClientIdPacketHandler;
use Kicken\Gearman\Server\PacketHandler\ClientPacketHandler;
use Kicken\Gearman\Server\PacketHandler\OptionsPacketHandler;
use Kicken\Gearman\Server\PacketHandler\WorkerPacketHandler;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;

class Server implements LoggerAwareInterface {
    /** @var Endpoint[] */
    private array $endpointList;
    private ServiceContainer $services;
    private bool $isShutdown = false;
    private string $handlePrefix;

    public function __construct($endpointList = '127.0.0.1:4730', string $handlePrefix = null, ServiceContainer $services = null){
        if (!is_array($endpointList)){
            $endpointList = [$endpointList];
        }

        $this->services = $services ?? new ServiceContainer();
        $this->endpointList = mapToEndpointObjects($endpointList, $this->services);
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
     */
    public function setLogger(LoggerInterface $logger) : self{
        $this->services->logger = $logger;

        return $this;
    }

    /**
     * Start listening on the configured endpoints for client connections.
     *
     * @return void
     */
    public function run() : void{
        $this->services->eventDispatcher->addListener(ClientDisconnected::class, function(ClientDisconnected $event){
            $this->services->logger->info('Lost connection to ' . $event->client->getAddress());
            $worker = $this->services->workerManager->getWorker($event->client);
            $job = $worker->getCurrentJob();
            if ($job){
                $job->running = false;
                $this->services->logger->notice('Re-queuing active job', ['handle' => $job->jobHandle]);
                $this->services->jobQueue->requeue($job);
            }
            $this->services->workerManager->removeConnection($event->client);
        });

        foreach ($this->endpointList as $endpoint){
            $this->services->logger->info('Listening on ' . $endpoint->getAddress());
            $endpoint->listen(function(Endpoint $stream){
                $this->services->logger->info('Received connection from ' . $stream->getAddress());
                $this->services->eventDispatcher->dispatch(new Events\ClientConnected($stream));
                $stream->addPacketHandler(new AdminPacketHandler($this, $this->services));
                $stream->addPacketHandler(new ClientIdPacketHandler());
                $stream->addPacketHandler(new OptionsPacketHandler());
                $stream->addPacketHandler(new ClientPacketHandler($this->services, $this->handlePrefix));
                $stream->addPacketHandler(new WorkerPacketHandler($this, $this->services));
            });
        }

        $this->services->loop->run();
    }

    /**
     * Stop accepting new client connections and shutdown the server.
     *
     * @param bool $graceful Let current worker clients continue their jobs.
     *
     * @return void
     */
    public function shutdown(bool $graceful = true) : void{
        $this->services->logger->notice('Server shutdown requested.', ['graceful' => $graceful]);
        $this->isShutdown = true;
        foreach ($this->endpointList as $endpoint){
            $endpoint->shutdown();
        }

        if (!$graceful){
            $this->services->workerManager->disconnectAll();
        } else {
            $this->services->workerManager->disconnectSleeping();
        }
    }
}
