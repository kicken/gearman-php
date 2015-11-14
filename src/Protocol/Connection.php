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
     */
    public function connect(){
        if (empty($this->serverList)){
            throw new EmptyServerListException;
        }

        foreach ($this->serverList as $uri){
            $stream = $this->tryServer($uri);
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
     * @returns Packet
     */
    public function readPacket(){
        if (!$this->stream){
            $this->connect();
        }

        $header = $this->read(12);

        $size = substr($header, 8, 4);
        $size = Packet::fromBigEndian($size);

        $arguments = $size > 0?$this->read($size):'';

        return Packet::fromString($header . $arguments);
    }

    /**
     * Send a single packet to the gearman server.
     *
     * Blocks until the packet has been sent.
     *
     * @param Packet $packet
     */
    public function writePacket(Packet $packet){
        if (!$this->stream){
            $this->connect();
        }

        $this->write((string)$packet);
    }

    private function write($data){
        if (!$this->stream || feof($this->stream)){
            throw new NotConnectedException;
        }

        $written = fwrite($this->stream, $data);
        if ($written === 0){
            throw new LostConnectionException;
        }

        fflush($this->stream);
    }

    private function read($length){
        if (!$this->stream || feof($this->stream)){
            throw new NotConnectedException;
        }

        do {
            $data = fread($this->stream, $length);
            if ($data === '' && feof($this->stream)){
                throw new LostConnectionException;
            }
        } while ($data === '');

        return $data;
    }

    private function tryServer($uri){
        $stream = stream_socket_client($uri, $errno, $errstr);
        if ($stream){
            stream_set_blocking($stream, true);
            stream_set_read_buffer($stream, 0);
            stream_set_write_buffer($stream, 0);
        }

        return $stream;
    }
}
