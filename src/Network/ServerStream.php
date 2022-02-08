<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use Kicken\Gearman\Protocol\PacketBuffer;
use React\EventLoop\LoopInterface;

class ServerStream {
    /** @var resource */
    private $stream;
    private LoopInterface $loop;
    private PacketBuffer $readBuffer;
    /** @var PacketHandler[] */
    private array $handlerList = [];

    public function __construct($stream, LoopInterface $loop){
        $this->stream = $stream;
        $this->loop = $loop;
        $this->readBuffer = new PacketBuffer();

        stream_set_blocking($stream, false);
        $loop->addReadStream($stream, function(){
            $this->buffer();
            $this->emitPackets();
        });
    }

    public function addPacketHandler(PacketHandler $handler){
        $this->handlerList[] = $handler;
    }

    private function buffer(){
        do {
            $data = fread($this->stream, 8192);
            if ($data === ''){
                $data = false;
            }

            if ($data){
                $this->readBuffer->feed($data);
            }
        } while ($data !== false);

        if (feof($this->stream)){
            $this->loop->removeReadStream($this->stream);
            fclose($this->stream);
        }
    }

    private function emitPackets(){
        while ($packet = $this->readBuffer->readPacket()){
            $handlerQueue = $this->handlerList;
            do {
                $handler = array_shift($handlerQueue);
            } while ($handler && !$handler->handlePacket(null, $packet));
        }
    }
}
