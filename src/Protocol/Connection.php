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

namespace Kicken\Gearman\Protocol;

use Kicken\Gearman\Exception\CouldNotConnectException;
use Kicken\Gearman\Exception\EmptyServerListException;
use Kicken\Gearman\Exception\LostConnectionException;
use Kicken\Gearman\Exception\NotConnectedException;
use Kicken\Gearman\Exception\TimeoutException;

/**
 * A connection to one of several possible Gearman servers.
 *
 * @package Kicken\Gearman\Protocol
 */
class Connection {
    /**
     * @var string[]
     */
    private $serverList;

    /**
     * @var resource
     */
    private $stream;

    /**
     * Create a connection to one of several possible gearman servers.
     *
     * When connecting, each server will be tried in order. The first server to connect successfully will be used.
     *
     * @param array $serverList A list of servers to try
     */
    public function __construct($serverList){
        $this->serverList = $serverList;
    }

    /**
     * Attempt to connect to the gearman server.
     * Attempts to connect to each server in the list until one connects successfully.
     *
     * @param int|bool $timeout
     */
    public function connect($timeout = false){
        if (empty($this->serverList)){
            throw new EmptyServerListException;
        }

        foreach ($this->serverList as $uri){
            $stream = $this->tryServer($uri, $timeout);
            if ($stream){
                $this->stream = $stream;
                break;
            }
        }

        if (!$this->stream){
            throw new CouldNotConnectException;
        }
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

        $arguments = $size > 0?$this->read($size, $timeout):'';

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
        if (!$this->stream){
            $this->connect($timeout);
        }

        $this->write((string)$packet, $timeout);
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
        do {
            $written = fwrite($this->stream, $data);
            $end = (int)(microtime(true) * 1000);
            if ($written === 0){
                if (feof($this->stream)){
                    throw new LostConnectionException;
                } else if ($timeout !== false && $end - $start >= $timeout){
                    throw new TimeoutException;
                }
            }
        } while ($written === 0);

        fflush($this->stream);
    }

    private function read($length, $timeout = false){
        if (!$this->stream || feof($this->stream)){
            throw new NotConnectedException;
        }

        $start = (int)(microtime(true) * 1000);
        do {
            if ($timeout !== false){
                $sec = (int)($timeout / 1000);
                $usec = ($timeout % 1000) * 1000;
                stream_set_timeout($this->stream, $sec, $usec);
            }

            $data = '';
            do {
                $data .= fread($this->stream, $length - strlen($data));
            } while (strlen($data) < $length && !feof($this->stream));

            $end = (int)(microtime(true) * 1000);
            if ($data === ''){
                if (feof($this->stream)){
                    throw new LostConnectionException;
                } else if ($timeout !== false && $end - $start >= $timeout){
                    throw new TimeoutException;
                }
            }
        } while ($data === '');

        return $data;
    }

    private function tryServer($uri, $timeout){
        if ($timeout === false){
            $timeout = -1;
        } else {
            $timeout /= 1000;
        }

        $stream = stream_socket_client($uri, $errno, $errstr, $timeout);
        if ($stream){
            stream_set_blocking($stream, true);
            stream_set_read_buffer($stream, 0);
            stream_set_write_buffer($stream, 0);
        }

        return $stream;
    }
}
