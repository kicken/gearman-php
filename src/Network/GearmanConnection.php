<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Exception\NotConnectedException;
use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketBuffer;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class GearmanConnection implements Connection {
    /** @var resource */
    private $stream;

    /** @var PacketHandler[] */
    private array $handlerList = [];

    private LoopInterface $loop;
    private string $writeBuffer = '';
    private PacketBuffer $readBuffer;

    public function __construct($stream, LoopInterface $loop = null){
        $this->stream = $stream;
        $this->loop = $loop ?? Loop::get();
        $this->readBuffer = new PacketBuffer();

        stream_set_blocking($this->stream, false);
        $this->loop->addReadStream($this->stream, function(){
            $this->buffer();
            $this->emitPackets();
        });
    }

    public function getFd() : int{
        return (int)$this->stream;
    }

    public function getRemoteAddress() : string{
        return stream_socket_get_name($this->stream, true);
    }

    public function isConnected() : bool{
        return $this->stream !== null;
    }

    public function writePacket(Packet $packet) : void{
        $this->writeBuffer .= $packet;
        $this->flush();
    }

    public function disconnect() : void{
        if ($this->stream){
            $this->loop->removeReadStream($this->stream);
            fclose($this->stream);
            $this->stream = null;
        }
    }

    public function addPacketHandler(PacketHandler $handler) : void{
        $this->handlerList[] = $handler;
    }

    public function removePacketHandler(PacketHandler $handler) : void{
        $key = array_search($handler, $this->handlerList, true);
        if ($key !== false){
            unset($this->handlerList[$key]);
            if (!$this->handlerList){
                $this->disconnect();
            }
        }
    }

    private function flush() : void{
        if (!$this->stream){
            throw new NotConnectedException();
        }

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

        if (feof($this->stream)){
            $this->loop->removeReadStream($this->stream);
            fclose($this->stream);
            $this->stream = null;
        }
    }

    private function emitPackets(){
        while ($packet = $this->readBuffer->readPacket()){
            $handlerQueue = $this->handlerList;
            $handled = false;
            do {
                $handler = array_shift($handlerQueue);
            } while ($handler && !($handled = $handler->handlePacket($this, $packet)));

            if (!$handled){
                echo 'Unhandled Packet ' . get_class($packet) . ': "' . $this->encodePacket($packet) . '"', PHP_EOL;
            }
        }
    }

    private function encodePacket(string $packet){
        return preg_replace_callback('/[^A-Za-z0-9]/', function($v){
            return '\x' . bin2hex($v[0]);
        }, $packet);
    }
}
