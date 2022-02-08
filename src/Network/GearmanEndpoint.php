<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Exception\CouldNotConnectException;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class GearmanEndpoint implements Endpoint {
    private string $url;
    private LoopInterface $loop;
    /** @var resource */
    private $stream = null;

    public function __construct(string $url, LoopInterface $loop = null){
        $this->url = $url;
        $this->loop = $loop ?? Loop::get();
    }

    public function listen(callable $handler){
        $this->stream = stream_socket_server($this->url, $errNo, $errStr);
        if (!$this->stream){
            throw new CouldNotConnectException();
        }

        $this->loop->addReadStream($this->stream, function() use ($handler){
            $this->accept($handler);
        });
    }

    private function accept(callable $handler){
        $connection = stream_socket_accept($this->stream);
        if ($connection){
            call_user_func($handler, new ServerStream($connection, $this->loop));
        }
    }

}