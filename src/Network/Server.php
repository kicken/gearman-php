<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Protocol\Packet;
use React\EventLoop\LoopInterface;

class Server {
    /** @var resource */
    private $stream;

    /** @var callable */
    private $packetHandler = null;

    private LoopInterface $loop;
    private string $writeBuffer = '';
    private string $readBuffer = '';

    public function __construct($stream, LoopInterface $loop){
        $this->stream = $stream;
        $this->loop = $loop;
        stream_set_blocking($this->stream, false);
        $this->loop->addReadStream($this->stream, function(){
            $this->buffer();
        });
    }

    public function writePacket(Packet $packet) : void{
        $this->writeBuffer .= $packet;
        $this->flush();
    }

    public function onPacketReceived(callable $handler) : void{
        $this->packetHandler = $handler;
    }

    private function flush() : void{
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

    private function buffer() : void{
        do {
            $data = fread($this->stream, 8192);
            if ($data){
                $this->readBuffer .= $data;
            }
        } while ($data);

        $packet = Packet::fromString($this->readBuffer);
        $this->readBuffer = '';

        if ($this->packetHandler){
            call_user_func($this->packetHandler, $this, $packet);
        }
    }
}
