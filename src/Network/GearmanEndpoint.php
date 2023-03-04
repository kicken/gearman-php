<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Exception\CouldNotConnectException;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;
use function React\Promise\reject;

class GearmanEndpoint implements Endpoint {
    private string $url;
    private int $connectTimeout;
    private LoopInterface $loop;
    /** @var resource */
    private $stream = null;

    public function __construct(string $url, int $connectTimeout = null, LoopInterface $loop = null){
        $this->url = $url;
        $this->connectTimeout = $connectTimeout ?? ini_get('default_socket_timeout');
        $this->loop = $loop ?? Loop::get();
    }

    public function connect() : ExtendedPromiseInterface{
        $this->stream = stream_socket_client($this->url, $errno, $errStr, null, STREAM_CLIENT_ASYNC_CONNECT);
        if (!$this->stream){
            return reject(new CouldNotConnectException($this, $errno, $errStr));
        }

        $deferred = new Deferred();
        $timeoutTimer = $this->loop->addTimer($this->connectTimeout, function() use ($deferred){
            $this->completeConnectionAttempt($deferred);
        });

        $this->loop->addWriteStream($this->stream, function() use ($deferred, $timeoutTimer){
            $this->loop->cancelTimer($timeoutTimer);
            $this->completeConnectionAttempt($deferred);
        });

        return $deferred->promise();
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

    private function completeConnectionAttempt(Deferred $deferred){
        $this->loop->removeWriteStream($this->stream);
        if ($this->isStreamConnected()){
            $deferred->resolve(new GearmanConnection($this->stream, $this->loop));
        } else {
            $this->stream = null;
            $deferred->reject(new CouldNotConnectException($this));
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
}