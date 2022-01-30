<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketBuffer;
use React\EventLoop\LoopInterface;

class Server {
    /** @var resource */
    private $stream;

    /** @var callable */
    private $packetHandler = null;

    private LoopInterface $loop;
    private string $writeBuffer = '';
    private PacketBuffer $readBuffer;

    public function __construct($stream, LoopInterface $loop){
        $this->stream = $stream;
        $this->loop = $loop;
        $this->readBuffer = new PacketBuffer();
        stream_set_blocking($this->stream, false);
        $this->loop->addReadStream($this->stream, function(){
            $this->buffer();
            $this->emitPackets();
        });
    }

    public function writePacket(Packet $packet) : void{
        $this->writeBuffer .= $packet;
        $this->flush();
    }

    public function disconnect(){
        $this->loop->removeReadStream($this->stream);
        fclose($this->stream);
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
                $this->readBuffer->feed($data);
            }
        } while ($data);
    }

    private function emitPackets(){
        if ($this->packetHandler){
            while ($packet = $this->readBuffer->readPacket()){
                call_user_func($this->packetHandler, $this, $packet);
            }
        }
    }
}
