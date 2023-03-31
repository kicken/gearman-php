<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Events\EndpointEvents;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Server\JobQueue;
use Kicken\Gearman\Server\PacketHandler\AdminPacketHandler;
use Kicken\Gearman\Server\PacketHandler\ClientIdPacketHandler;
use Kicken\Gearman\Server\PacketHandler\ClientPacketHandler;
use Kicken\Gearman\Server\PacketHandler\OptionsPacketHandler;
use Kicken\Gearman\Server\PacketHandler\WorkerPacketHandler;
use Kicken\Gearman\Server\Statistics;
use Kicken\Gearman\Server\WorkerManager;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class Server {
    use LoggerAwareTrait {
        LoggerAwareTrait::setLogger as originalSetLogger;
    }

    /** @var Endpoint[] */
    private array $endpointList;
    private LoopInterface $loop;
    private WorkerManager $workerRegistry;
    private JobQueue $jobQueue;
    private Statistics $statistics;
    private bool $isShutdown = false;

    public function __construct($endpointList = '127.0.0.1:4730', LoopInterface $loop = null){
        if (!is_array($endpointList)){
            $endpointList = [$endpointList];
        }

        $this->logger = new NullLogger();
        $this->loop = $loop ?? Loop::get();
        $this->endpointList = mapToEndpointObjects($endpointList, $this->loop);
        $this->workerRegistry = new WorkerManager();
        $this->jobQueue = new JobQueue($this->workerRegistry);
        $this->statistics = new Statistics($this->workerRegistry, $this->jobQueue, $this->logger);
    }

    public function isShutdown() : bool{
        return $this->isShutdown;
    }

    public function setJobHandlePrefix(string $prefix){
        $this->jobQueue->setHandlePrefix($prefix);
    }

    public function setLogger(LoggerInterface $logger){
        $this->originalSetLogger($logger);
        $this->statistics->setLogger($logger);
        foreach ($this->endpointList as $item){
            if ($item instanceof LoggerAwareInterface){
                $item->setLogger($this->logger);
            }
        }
    }

    public function run(){
        foreach ($this->endpointList as $endpoint){
            $this->logger->info('Listening on ' . $endpoint->getAddress());
            $endpoint->listen(function(Endpoint $stream){
                $this->logger->info('Received connection from ' . $stream->getAddress());
                $stream->addPacketHandler(new AdminPacketHandler($this, $this->statistics, $this->logger));
                $stream->addPacketHandler(new ClientIdPacketHandler());
                $stream->addPacketHandler(new OptionsPacketHandler());
                $stream->addPacketHandler(new ClientPacketHandler($this->jobQueue, $this->logger));
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
