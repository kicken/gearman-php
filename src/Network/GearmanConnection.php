<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Exception\LostConnectionException;
use Kicken\Gearman\Exception\NotConnectedException;
use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketBuffer;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class GearmanConnection implements Connection {
    /** @var resource */
    private $stream;
    private string $remoteAddress;

    /** @var callable[] */
    private array $disconnectHandlerList = [];
    /** @var PacketHandler[] */
    private array $packetHandlerList = [];

    private LoopInterface $loop;
    private string $writeBuffer = '';
    private PacketBuffer $readBuffer;

    public function __construct($stream, LoopInterface $loop = null){
        $this->stream = $stream;
        $this->loop = $loop ?? Loop::get();
        $this->readBuffer = new PacketBuffer();
        $this->remoteAddress = stream_socket_get_name($this->stream, true);

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
        return $this->remoteAddress;
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
            $this->loop->removeWriteStream($this->stream);
            fclose($this->stream);
            $this->stream = null;
            foreach ($this->disconnectHandlerList as $handler){
                call_user_func($handler, $this);
            }
        }
    }

    public function addPacketHandler(PacketHandler $handler) : void{
        $this->packetHandlerList[] = $handler;
    }

    public function removePacketHandler(PacketHandler $handler) : void{
        $key = array_search($handler, $this->packetHandlerList, true);
        if ($key !== false){
            unset($this->packetHandlerList[$key]);
            if (!$this->packetHandlerList){
                $this->disconnect();
            }
        }
    }

    public function addDisconnectHandler(callable $handler) : void{
        $this->disconnectHandlerList[] = $handler;
    }

    private function flush() : void{
        if (!$this->stream){
            throw new NotConnectedException();
        }

        set_error_handler(function($errNo){
            if ($errNo == E_NOTICE){
                throw new LostConnectionException();
            }
        });
        $written = fwrite($this->stream, $this->writeBuffer);
        restore_error_handler();
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
            $this->disconnect();
        }
    }

    private function emitPackets(){
        try {
            while ($packet = $this->readBuffer->readPacket()){
                $handlerQueue = $this->packetHandlerList;
                $handled = false;
                do {
                    $handler = array_shift($handlerQueue);
                } while ($handler && !($handled = $handler->handlePacket($this, $packet)));

                if (!$handled){
                    echo 'Unhandled Packet ' . get_class($packet) . ': "' . $this->encodePacket($packet) . '"', PHP_EOL;
                }
            }
        } catch (LostConnectionException $ex){
            $this->disconnect();
        }
    }

    private function encodePacket(string $packet){
        return preg_replace_callback('/[^A-Za-z0-9]/', function($v){
            return '\x' . bin2hex($v[0]);
        }, $packet);
    }
}
