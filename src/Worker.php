<?php
/**
 * Copyright (c) 2015 Keith
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN Server WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

namespace Kicken\Gearman;


use Kicken\Gearman\Exception\LostConnectionException;
use Kicken\Gearman\Exception\NoRegisteredFunctionException;
use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Worker\PacketHandler\GrabJobHandler;
use Kicken\Gearman\Worker\WorkerJob;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Promise\ExtendedPromiseInterface;
use function React\Promise\any;

/**
 * A class for registering functions with and waiting for work from a Gearman server.
 *
 * @package Kicken\Gearman
 */
class Worker {
    use LoggerAwareTrait;

    /** @var Endpoint[] */
    private array $serverList;
    private LoopInterface $loop;
    private array $workerList = [];
    private bool $stop = false;

    /**
     * Create a new Gearman Worker to process jobs submitted to the server by clients.
     *
     * @param string|string[]|Endpoint|Endpoint[] $serverList The server(s) to connect to.
     * @param ?LoopInterface $loop Event loop implementation to use
     */
    public function __construct($serverList = '127.0.0.1:4730', LoopInterface $loop = null){
        $this->loop = $loop ?? Loop::get();
        $this->serverList = mapToEndpointObjects($serverList, $this->loop);
        $this->logger = new NullLogger();
    }

    /**
     * Register a function with the server.
     *
     * @param string $name The name of the function.
     * @param callable $callback A callback to be executed when a job is received.
     * @param int|null $timeout A time limit on how the server should wait for a response.
     *
     * @return $this
     */
    public function registerFunction(string $name, callable $callback, int $timeout = null) : self{
        $this->workerList[$name] = [
            'name' => $name,
            'timeout' => $timeout,
            'callback' => $callback
        ];

        return $this;
    }

    /**
     * Go into a loop accepting jobs and performing the work.
     */
    public function work() : void{
        if (empty($this->workerList)){
            throw new NoRegisteredFunctionException;
        }

        $this->workAsync();
        $this->loop->run();
    }

    /**
     * Begin the process of accepting jobs while allowing the main script to continue.
     * Main script must run the main loop at some future point.
     *
     * @noinspection PhpIncompatibleReturnTypeInspection
     */
    public function workAsync() : ExtendedPromiseInterface{
        $allPromises = [];
        foreach ($this->serverList as $server){
            $allPromises[] = $server->connect()->then(function(Connection $server){
                $this->registerFunctionsWithServer($server);
                $this->grabJob($server);
            });
        }

        return any($allPromises);
    }

    /**
     * Stop accepting new jobs. Any job currently in progress will be completed.
     */
    public function stopWorking() : void{
        $this->stop = true;
    }

    private function registerFunctionsWithServer(Connection $server) : void{
        foreach ($this->workerList as $item){
            if ($item['timeout'] === null){
                $packet = new BinaryPacket(PacketMagic::REQ, PacketType::CAN_DO, [$item['name']]);
            } else {
                $packet = new BinaryPacket(PacketMagic::REQ, PacketType::CAN_DO_TIMEOUT, [$item['name'], $item['timeout']]);
            }

            $server->writePacket($packet);
        }
    }

    private function grabJob(Connection $server) : void{
        if (!$this->stop && $server->isConnected()){
            (new GrabJobHandler($this->logger))->grabJob($server)->then(function(WorkerJob $job) use ($server){
                $this->processJob($job);
                $this->grabJob($server);
            })->done();
        }
    }

    private function processJob(WorkerJob $job) : void{
        if (!isset($this->workerList[$job->getFunction()])){
            throw new NoRegisteredFunctionException;
        }

        $worker = $this->workerList[$job->getFunction()]['callback'];
        try {
            $result = call_user_func($worker, $job);
            if ($result === false){
                $job->sendFail();
            } else {
                $job->sendComplete((string)$result);
            }
        } catch (LostConnectionException $e){
            throw $e;
        } catch (\Exception $e){
            $job->sendException(get_class($e) . ': ' . $e->getMessage());
        }
    }
}
