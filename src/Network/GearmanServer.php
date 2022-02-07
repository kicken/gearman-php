<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Exception\CouldNotConnectException;
use Kicken\Gearman\Exception\NotConnectedException;
use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketBuffer;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;
use function React\Promise\reject;

class GearmanServer implements Server {
    private string $url;
    private int $connectTimeout;
    /** @var resource */
    private $stream = null;

    /** @var PacketHandler[] */
    private array $handlerList = [];

    private LoopInterface $loop;
    private string $writeBuffer = '';
    private PacketBuffer $readBuffer;

    public function __construct(string $url, int $connectTimeout = null, LoopInterface $loop = null){
        $this->url = $url;
        $this->connectTimeout = $connectTimeout ?? ini_get('default_socket_timeout');
        $this->loop = $loop ?? Loop::get();
        $this->readBuffer = new PacketBuffer();
    }

    public function connect() : ExtendedPromiseInterface{
        $this->stream = stream_socket_client($this->url, $errno, $errStr, null, STREAM_CLIENT_ASYNC_CONNECT);
        if (!$this->stream){
            return reject(new CouldNotConnectException());
        }

        $deferred = new Deferred();
        $timeoutTimer = $this->loop->addTimer($this->connectTimeout, function() use ($deferred){
            $this->completeConnectionAttempt($deferred);
        });

        $this->loop->addWriteStream($this->stream, function() use ($deferred, $timeoutTimer){
            $this->loop->cancelTimer($timeoutTimer);
            $this->completeConnectionAttempt($deferred);
        });

        return $deferred->promise();
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
    }

    private function emitPackets(){
        while ($packet = $this->readBuffer->readPacket()){
            $handlerQueue = $this->handlerList;
            do {
                $handler = array_shift($handlerQueue);
            } while ($handler && !$handler->handlePacket($this, $packet));
        }
    }

    private function completeConnectionAttempt(Deferred $deferred){
        $this->loop->removeWriteStream($this->stream);
        if ($this->isStreamConnected()){
            stream_set_blocking($this->stream, false);
            $this->loop->addReadStream($this->stream, function(){
                $this->buffer();
                $this->emitPackets();
            });
            $deferred->resolve($this);
        } else {
            $this->stream = null;
            $deferred->reject(new CouldNotConnectException());
        }
    }

    private function isStreamConnected() : bool{
        if (!$this->stream){
            return false;
        }

        //If there's no remote end to the socket we failed.
        $remote = stream_socket_get_name($this->stream, true);
        if (!$remote){
            return false;
        }

        return true;
    }
}
