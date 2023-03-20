<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Exception\CouldNotConnectException;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Promise\Promise;
use React\Promise\PromiseInterface;
use function React\Promise\reject;
use function React\Promise\resolve;

class GearmanEndpoint implements Endpoint {
    use LoggerAwareTrait;

    private string $url;
    private int $connectTimeout;
    private LoopInterface $loop;
    /** @var resource */
    private $stream = null;
    private ?Connection $connection = null;
    private ?PromiseInterface $connectingPromise = null;

    public function __construct(string $url, int $connectTimeout = null, LoopInterface $loop = null){
        $this->url = $url;
        $this->connectTimeout = $connectTimeout ?? ini_get('default_socket_timeout');
        $this->loop = $loop ?? Loop::get();
        $this->logger = new NullLogger();
    }

    public function connect(bool $autoDisconnect) : PromiseInterface{
        if ($this->connection && $this->connection->isConnected()){
            $this->connection->setAutoDisconnect($autoDisconnect);

            return resolve($this->connection);
        }

        if ($this->connectingPromise){
            return $this->connectingPromise;
        }

        $this->connection = null;
        $this->stream = stream_socket_client($this->url, $errno, $errStr, null, STREAM_CLIENT_ASYNC_CONNECT);
        if (!$this->stream){
            return reject(new CouldNotConnectException($this, $errno, $errStr));
        }

        $promise = new Promise(function($resolve, $reject){
            $timeoutTimer = $this->loop->addTimer($this->connectTimeout, function() use ($resolve, $reject){
                $this->completeConnectionAttempt($resolve, $reject);
            });

            $this->loop->addWriteStream($this->stream, function() use ($resolve, $reject, $timeoutTimer){
                $this->loop->cancelTimer($timeoutTimer);
                $this->completeConnectionAttempt($resolve, $reject);
            });
        });

        $promise = $promise->then(function(Connection $connection) use ($autoDisconnect){
            $this->logger->info('Successfully connected to server', ['endpoint' => $connection->getRemoteAddress()]);
            $connection->setAutoDisconnect($autoDisconnect);

            return $this->connection = $connection;
        }, function($error){
            $this->logger->warning($error->getMessage(), ['url' => $this->url]);
            throw $error;
        });

        return $this->connectingPromise = $promise;
    }

    public function disconnect() : void{
        if ($this->connection){
            $this->connection->disconnect();
        }
    }

    public function getAddress() : string{
        return $this->url;
    }

    public function listen(callable $handler){
        $this->stream = stream_socket_server($this->url, $errNo, $errStr);
        if (!$this->stream){
            throw new CouldNotConnectException($this, $errNo, $errStr);
        }

        $this->loop->addReadStream($this->stream, function() use ($handler){
            $this->accept($handler);
        });
    }

    private function accept(callable $handler){
        $connection = stream_socket_accept($this->stream);
        if ($connection){
            call_user_func($handler, new GearmanConnection($connection, $this->loop));
        }
    }

    private function completeConnectionAttempt(callable $resolve, callable $reject){
        $this->loop->removeWriteStream($this->stream);
        if ($this->isStreamConnected()){
            $resolve(new GearmanConnection($this->stream, $this->loop));
        } else {
            $this->stream = null;
            $reject(new CouldNotConnectException($this));
        }
    }

    private function isStreamConnected() : bool{
        if (!$this->stream){
            return false;
        }

        //If there's no remote end to the socket we failed.
        $remote = stream_socket_get_name($this->stream, true);
        if (!$remote){
            return false;
        }

        return true;
    }

    public function shutdown(){
        $this->loop->removeWriteStream($this->stream);
        $this->loop->removeReadStream($this->stream);
        stream_socket_shutdown($this->stream, STREAM_SHUT_RDWR);
        fclose($this->stream);
        $this->stream = null;
    }
}
