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
use Kicken\Gearman\Exception\LostConnectionException;
use Kicken\Gearman\Exception\NotConnectedException;
use Kicken\Gearman\Exception\TimeoutException;
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

    /**
     * Create a connection to one of several possible gearman servers.
     *
     * When connecting, each server will be tried in order. The first server to connect successfully will be used.
     *
     * @param array $serverList A list of servers to try
     */
    public function __construct(array $serverList, LoopInterface $loop){
        $this->possibleServerList = $serverList;
        $this->loop = $loop;
    }

    /**
     * Attempt to connect to the servers in the list.
     *
     * @param ?int $timeout = null
     */
    public function connect(int $timeout = null){
        if (empty($this->possibleServerList)){
            throw new EmptyServerListException;
        }

        $streamList = [];
        $connectedStreamList = [];
        foreach ($this->possibleServerList as $url){
            $stream = $this->initiateConnection($url);
            if (!$stream){
                continue;
            }

            $streamList[] = $stream;
            $this->loop->addWriteStream($stream, function($stream) use (&$connectedStreamList){
                $this->loop->removeWriteStream($stream);

                $r = $e = [];
                $w = [$stream];
                $n = stream_select($r, $w, $e, 0);
                if ($n === 1){
                    $connectedStreamList[] = $stream;
                }
            });
        }

        if ($timeout){
            $this->loop->addTimer($timeout, function() use (&$streamList){
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
            return new Server($stream, $this->loop);
        }, $connectedStreamList);
    }

    /**
     * Read a single packet from the gearman server.
     *
     * Blocks if no packet is available
     *
     * @param int|bool $timeout
     *
     * @returns Packet
     */
    public function readPacket($timeout = false){
        if (!$this->stream){
            $this->connect($timeout);
        }

        $header = $this->read(12, $timeout);

        $size = substr($header, 8, 4);
        $size = Packet::fromBigEndian($size);

        $arguments = $size > 0 ? $this->read($size, $timeout) : '';

        return Packet::fromString($header . $arguments);
    }

    /**
     * Send a single packet to the gearman server.
     *
     * Blocks until the packet has been sent.
     *
     * @param Packet $packet
     * @param int|bool $timeout
     */
    public function writePacket(Packet $packet, $timeout = false){
        if (!$this->connectedServerList){
            $this->connect();
        }

        foreach ($this->connectedServerList as $server){
            $server->writePacket($packet);
        }
    }

    private function write($data, $timeout){
        if (!$this->stream || feof($this->stream)){
            throw new NotConnectedException;
        }

        if ($timeout !== false){
            $sec = (int)($timeout / 1000);
            $usec = ($timeout % 1000) * 1000;
            stream_set_timeout($this->stream, $sec, $usec);
        }

        $start = (int)(microtime(true) * 1000);
        $remaining = strlen($data);
        do {
            $this->pushErrorHandler();
            $written = fwrite($this->stream, $data);
            $this->popErrorHandler();
            $end = (int)(microtime(true) * 1000);
            if ($written === 0){
                if ($timeout !== false && $end - $start >= $timeout){
                    throw new TimeoutException;
                } else {
                    throw new LostConnectionException;
                }
            } else {
                $remaining -= $written;
                $data = substr($data, $written);
            }
        } while ($written !== 0 && $remaining > 0);

        fflush($this->stream);
    }

    private function read($length, $timeout = false){
        if (!$this->stream || feof($this->stream)){
            throw new NotConnectedException;
        }

        $start = (int)(microtime(true) * 1000);
        $data = '';
        $dataLength = 0;
        $timeUsed = 0;
        do {
            if ($timeout !== false){
                $timeRemaining = $timeout - $timeUsed;
                $sec = (int)($timeRemaining / 1000);
                $usec = ($timeRemaining % 1000) * 1000;
                stream_set_timeout($this->stream, $sec, $usec);
            }

            $amountToRead = $length - strlen($data);
            $readResult = fread($this->stream, $amountToRead);
            if (is_string($readResult)){
                $data .= $readResult;
                $dataLength = strlen($data);
            }

            $end = (int)(microtime(true) * 1000);
            if ($dataLength !== $length){
                $timeUsed += $end - $start;

                if (feof($this->stream)){
                    throw new LostConnectionException;
                } else if ($timeout !== false && $timeUsed >= $timeout){
                    throw new TimeoutException;
                }
            }
        } while ($dataLength < $length);

        return $data;
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

    private function pushErrorHandler(){
        /** @noinspection PhpUnusedLocalVariableInspection */
        $previousErrorHandler = set_error_handler(function($errno, $errstr) use (&$previousErrorHandler){
            if (preg_match('/errno=\d+/', $errstr, $matches)){
                return true;
            } else {
                return call_user_func_array($previousErrorHandler, func_get_args());
            }
        }, E_NOTICE);
    }

    private function popErrorHandler(){
        restore_error_handler();
    }
}
