<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Server\JobQueue;
use Kicken\Gearman\Server\PacketHandler\AdminPacketHandler;
use Kicken\Gearman\Server\PacketHandler\ClientPacketHandler;
use Kicken\Gearman\Server\PacketHandler\WorkerPacketHandler;
use Kicken\Gearman\Server\WorkerManager;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class Server {
    use LoggerAwareTrait;

    /** @var Endpoint[] */
    private array $endpointList;
    private LoopInterface $loop;
    private WorkerManager $workerRegistry;
    private JobQueue $jobQueue;

    public function __construct($endpointList = '127.0.0.1:4730', LoopInterface $loop = null){
        if (!is_array($endpointList)){
            $endpointList = [$endpointList];
        }

        $this->logger = new NullLogger();
        $this->loop = $loop ?? Loop::get();
        $this->endpointList = mapToEndpointObjects($endpointList, $this->loop);
        $this->workerRegistry = new WorkerManager();
        $this->jobQueue = new JobQueue($this->workerRegistry);
    }

    public function run(){
        foreach ($this->endpointList as $endpoint){
            $this->logger->info('Listening on ' . $endpoint->getAddress());
            $endpoint->listen(function(Connection $stream){
                $this->logger->info('Received connection from ' . $stream->getRemoteAddress());
                $stream->addPacketHandler(new AdminPacketHandler($this->workerRegistry, $this->logger));
                $stream->addPacketHandler(new ClientPacketHandler($this->jobQueue, $this->logger));
                $stream->addPacketHandler(new WorkerPacketHandler($this->workerRegistry, $this->jobQueue, $this->logger));
                $stream->addDisconnectHandler(function(Connection $connection){
                    $this->logger->info('Lost connection to ' . $connection->getRemoteAddress());
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
}
