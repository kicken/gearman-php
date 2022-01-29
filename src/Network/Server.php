<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Protocol\Packet;
use React\EventLoop\LoopInterface;

class Server {
    /** @var resource */
    private $stream;

    /** @var callable */
    private $packetHandler;

    /** @var LoopInterface */
    private $loop;

    private string $writeBuffer = '';
    private string $readBuffer = '';

    public function __construct($stream, callable $packetHandler, LoopInterface $loop){
        $this->stream = $stream;
        $this->packetHandler = $packetHandler;
        $this->loop = $loop;

        stream_set_blocking($this->stream, false);
        $this->loop->addReadStream($this->stream, function(){
            $this->buffer();
        });
    }

    public function writePacket(Packet $packet){
        $this->writeBuffer .= $packet;
        $this->flush();
    }

    private function flush(){
        $written = fwrite($this->stream, $this->writeBuffer);
        if ($written === strlen($this->writeBuffer)){
            $this->writeBuffer = '';
        } else {
            $this->writeBuffer = substr($this->writeBuffer, $written);
            $this->loop->addWriteStream($this->stream, function(){
                $this->loop->removeWriteStream($this->stream);
                $this->flush();
            });
        }
    }

    private function buffer(){
        do {
            $data = fread($this->stream, 8192);
            if ($data){
                $this->readBuffer .= $data;
            }
        } while ($data);

        $packet = Packet::fromString($this->readBuffer);
        $this->readBuffer = '';

        call_user_func($this->packetHandler, $this, $packet);
    }
}
