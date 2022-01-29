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
use Kicken\Gearman\Exception\NotConnectedException;
use Kicken\Gearman\Protocol\Packet;
use React\EventLoop\LoopInterface;

/**
 * A connection to one of several possible Gearman servers.
 *
 * @package Kicken\Gearman\Protocol
 */
class ServerPool {
    /**
     * @var string[]
     */
    private $possibleServerList;

    /**
     * @var LoopInterface
     */
    private $loop;

    /**
     * @var Server[]
     */
    private $connectedServerList = [];

    private $connectHandler;

    private $packetHandler;

    private $timeout;

    /**
     * Create a connection to one of several possible gearman servers.
     *
     * When connecting, each server will be tried in order. The first server to connect successfully will be used.
     *
     * @param array $serverList A list of servers to try
     */
    public function __construct(array $serverList, callable $connectHandler, callable $packetHandler, int $timeout, LoopInterface $loop){
        $this->possibleServerList = $serverList;
        $this->loop = $loop;
        $this->connectHandler = $connectHandler;
        $this->packetHandler = $packetHandler;
        $this->timeout = $timeout;
        $this->connect();
    }

    /**
     * Attempt to connect to the servers in the list.
     */
    private function connect(){
        if (empty($this->possibleServerList)){
            throw new EmptyServerListException;
        }

        $streamList = [];
        $connectedStreamList = [];
        $timeoutTimer = null;
        foreach ($this->possibleServerList as $url){
            $stream = $this->initiateConnection($url);
            if (!$stream){
                continue;
            }

            $streamList[] = $stream;
            $this->loop->addWriteStream($stream, function($stream) use (&$streamList, &$connectedStreamList, &$timeoutTimer){
                $this->loop->removeWriteStream($stream);

                $index=array_search($stream, $streamList);
                unset($streamList[$index]);
                if(!$streamList){
                    $this->loop->cancelTimer($timeoutTimer);
                }

                $r = $e = [];
                $w = [$stream];
                $n = stream_select($r, $w, $e, 0);
                if ($n === 1){
                    $connectedStreamList[] = $stream;
                }
            });
        }

        if ($this->timeout){
            $timeoutTimer = $this->loop->addTimer($this->timeout, function() use (&$streamList){
                foreach ($streamList as $stream){
                    $this->loop->removeWriteStream($stream);
                }

                if (!$this->connectedServerList){
                    throw new CouldNotConnectException();
                }
            });
        }

        $this->loop->run();
        if (!$connectedStreamList){
            throw new CouldNotConnectException();
        }

        $this->connectedServerList = array_map(function($stream){
            $server = new Server($stream, $this->packetHandler, $this->loop);
            call_user_func($this->connectHandler, $server);

            return $server;
        }, $connectedStreamList);
    }

    /**
     * Send a single packet to the gearman server.
     *
     * Blocks until the packet has been sent.
     *
     * @param Packet $packet
     * @param int|bool $timeout
     */
    public function writePacket(Packet $packet){
        if (!$this->connectedServerList){
            throw new NotConnectedException();
        }

        foreach ($this->connectedServerList as $server){
            $server->writePacket($packet);
        }
    }

    private function initiateConnection($uri){
        $stream = stream_socket_client($uri, $errno, $errStr, null, STREAM_CLIENT_ASYNC_CONNECT);
        if (!$stream){
            return false;
        }

        stream_set_blocking($stream, true);
        stream_set_read_buffer($stream, 0);
        stream_set_write_buffer($stream, 0);

        return $stream;
    }
}
