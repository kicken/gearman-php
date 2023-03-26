<?php

namespace Kicken\Gearman\Test\Network;

use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketBuffer;
use React\EventLoop\LoopInterface;

class MockServer {
    private LoopInterface $loop;
    /** @var resource */
    private $serverStream = null;
    /** @var resource */
    private $client = null;
    /** @var Packet[] */
    private array $packetList = [];

    public function __construct(LoopInterface $loop){
        $this->loop = $loop;
    }

    public function listen() : string{
        $this->serverStream = stream_socket_server('127.0.0.1:0', $errNo, $errStr);
        if (!$this->serverStream){
            throw new \RuntimeException(sprintf('Unable to launch server. [%d] %s', $errNo, $errStr));
        }

        $this->loop->addReadStream($this->serverStream, function(){
            $buffer = new PacketBuffer();
            $this->client = stream_socket_accept($this->serverStream);
            $this->loop->addReadStream($this->client, function() use ($buffer){
                $buffer->feed(fread($this->client, 1024));
                while ($packet = $buffer->readPacket()){
                    $this->packetList[] = $packet;
                }
                if (feof($this->client)){
                    $this->disconnectClient();
                }
            });
        });

        return stream_socket_get_name($this->serverStream, false);
    }

    public function shutdown() : void{
        $this->disconnectClient();
        if (!$this->serverStream){
            return;
        }

        stream_socket_shutdown($this->serverStream, STREAM_SHUT_RDWR);
        fclose($this->serverStream);
        $this->loop->removeReadStream($this->serverStream);
        $this->serverStream = null;
        $this->client = null;
        $this->packetList = [];
    }

    public function receivedPacket(Packet $packet) : bool{
        foreach ($this->packetList as $item){
            if ($packet->__toString() === $item->__toString()){
                return true;
            }
        }

        return false;
    }

    public function writePacket(Packet $packet){
        if (!$this->client){
            throw new \RuntimeException('No client');
        }

        fwrite($this->client, $packet->__toString());
    }

    private function disconnectClient() : void{
        if (!$this->client){
            return;
        }
        stream_socket_shutdown($this->client, STREAM_SHUT_RDWR);
        fclose($this->client);
        $this->loop->removeReadStream($this->client);
        $this->client = null;
    }
}
