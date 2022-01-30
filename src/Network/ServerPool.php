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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Exception\CouldNotConnectException;
use Kicken\Gearman\Exception\EmptyServerListException;
use React\EventLoop\LoopInterface;

/**
 * A connection to one of several possible Gearman servers.
 *
 * @package Kicken\Gearman\Protocol
 */
class ServerPool {
    /** @var string[] */
    private array $possibleServerList;

    /** @var Server[] */
    private array $connectedServerList = [];

    private int $connectTimeout;

    private LoopInterface $loop;

    /**
     * Create a connection to one of several possible gearman servers.
     *
     * When connecting, each server will be tried in order. The first server to connect successfully will be used.
     *
     * @param array $serverList A list of servers to try
     * @param int $connectTimeout How long to wait for a connection to be established.
     * @param LoopInterface $loop Event loop used to monitor sockets for activity.
     */
    public function __construct(array $serverList, int $connectTimeout, LoopInterface $loop){
        $this->possibleServerList = $serverList;
        $this->loop = $loop;
        $this->connectTimeout = $connectTimeout;
    }

    /**
     * Attempt to connect to the servers in the list.
     *
     * @param callable $onConnect A callback that will be executed when a server has successfully connected.
     */
    public function connect(callable $onConnect) : void{
        if (empty($this->possibleServerList)){
            throw new EmptyServerListException;
        }

        $streamList = [];
        $timeoutTimer = $this->loop->addTimer($this->connectTimeout, function() use (&$streamList, $onConnect){
            foreach ($streamList as $stream){
                $this->loop->removeWriteStream($stream);
                if ($this->isStreamConnected($stream)){
                    $this->connectedServerList[] = new Server($stream, $this->loop);
                }
            }

            if (!$this->isConnected()){
                throw new CouldNotConnectException();
            } else {
                call_user_func($onConnect, $this->connectedServerList);
            }
        });

        foreach ($this->possibleServerList as $url){
            $stream = $this->initiateConnection($url);
            if (!$stream){
                continue;
            }

            $streamList[] = $stream;
            $this->loop->addWriteStream($stream, function($stream) use (&$streamList, $onConnect, $timeoutTimer){
                $this->loop->removeWriteStream($stream);
                $index = array_search($stream, $streamList);
                unset($streamList[$index]);
                if ($this->isStreamConnected($stream)){
                    $this->connectedServerList[] = new Server($stream, $this->loop);
                }

                if (!$streamList){
                    $this->loop->cancelTimer($timeoutTimer);
                    if ($this->isConnected()){
                        call_user_func($onConnect, $this->connectedServerList);
                    } else {
                        throw new CouldNotConnectException();
                    }
                }
            });
        }
    }

    public function isConnected() : bool{
        return count($this->connectedServerList) > 0;
    }

    public function randomServer() : Server{
        $key = array_rand($this->connectedServerList);

        return $this->connectedServerList[$key];
    }

    /** @noinspection PhpMissingReturnTypeInspection */
    private function initiateConnection(string $uri){
        $stream = stream_socket_client($uri, $errno, $errStr, null, STREAM_CLIENT_ASYNC_CONNECT);
        if (!$stream){
            return false;
        }

        stream_set_blocking($stream, true);
        stream_set_read_buffer($stream, 0);
        stream_set_write_buffer($stream, 0);

        return $stream;
    }

    private function isStreamConnected($stream) : bool{
        $r = $e = [];
        $w = [$stream];

        return stream_select($r, $w, $e, 0, 0) === 1;
    }
}
