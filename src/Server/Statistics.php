<?php

namespace Kicken\Gearman\Server;

use Kicken\Gearman\Events\ServerEvents;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use WeakReference;

class Statistics {
    use LoggerAwareTrait;

    private array $workerList = [];
    private array $functionQueueStats = [];

    public function __construct(WorkerManager $registry, JobQueue $jobQueue, LoggerInterface $logger){
        $this->logger = $logger;
        $registry->on(ServerEvents::WORKER_CONNECTED, function(Worker $worker){
            $this->logger->debug('New worker connected, Updating worker statistics.');
            $this->workerList[] = WeakReference::create($worker);
            $worker->on(ServerEvents::WORKER_REGISTERED_FUNCTION, function(string $function){
                $this->logger->debug('Incrementing available worker statistic for ' . $function . '.');
                $stats = &$this->getFunctionQueueStats($function);
                $stats['workers'] += 1;
            });
            $worker->on(ServerEvents::WORKER_UNREGISTERED_FUNCTION, function(string $function){
                $this->logger->debug('Decrementing available worker statistic for ' . $function . '.');
                $stats = &$this->getFunctionQueueStats($function);
                $stats['workers'] -= 1;
            });
            $worker->on(ServerEvents::JOB_STARTED, function(ServerJobData $job){
                $this->logger->debug('Incrementing running jobs statistic for ' . $job->function);
                $stats = &$this->getFunctionQueueStats($job->function);
                $stats['running'] += 1;
            });
            $worker->on(ServerEvents::JOB_STOPPED, function(ServerJobData $job){
                $this->logger->debug('Decrementing running jobs statistic for ' . $job->function);
                $stats = &$this->getFunctionQueueStats($job->function);
                $stats['running'] -= 1;
                $stats['total'] -= 1;
            });
        });

        $registry->on(ServerEvents::WORKER_DISCONNECTED, function(Worker $worker){
            $this->logger->debug('Worker disconnected, updating worker statistics.');
            $index = array_search(WeakReference::create($worker), $this->workerList, true);
            if ($index !== false){
                unset($this->workerList[$index]);
            }
            foreach ($worker->getAvailableFunctions() as $function){
                $stats = &$this->getFunctionQueueStats($function);
                $stats['workers'] -= 1;
            }
        });
        $jobQueue->on(ServerEvents::JOB_QUEUED, function(ServerJobData $job){
            $this->logger->debug('Incrementing total jobs statistic for ' . $job->function);
            $stats = &$this->getFunctionQueueStats($job->function);
            $stats['total'] += 1;
        });
    }

    public function listWorkerDetails() : string{
        $list = [];
        foreach ($this->workerList as $k => $ref){
            /** @var ?Worker $worker */
            $worker = $ref->get();
            if ($worker){
                $list[] = sprintf('%d %s %s: %s'
                    , $worker->getConnection()->getFd()
                    , $worker->getConnection()->getRemoteAddress()
                    , $worker->getClientId()
                    , implode(' ', $worker->getAvailableFunctions())
                );
            } else {
                unset($this->workerList[$k]);
            }
        }

        return $this->implodeList($list);
    }

    public function listQueueDetails() : string{
        $list = [];
        foreach ($this->functionQueueStats as $function => $details){
            $list[] = sprintf("%s\t%d\t%d\t%d"
                , $function, $details['total'], $details['running'], $details['workers']
            );
        }

        return $this->implodeList($list);
    }

    private function implodeList(array $list) : string{
        $response = implode("\r\n", $list);
        if ($response !== ''){
            $response .= "\r\n";
        }

        return $response . '.';
    }

    private function &getFunctionQueueStats(string $function) : array{
        $stats = &$this->functionQueueStats[$function];
        if ($stats === null){
            $stats = array_fill_keys(['total', 'running', 'workers'], 0);
        }

        return $stats;
    }
}
