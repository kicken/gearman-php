<?php

namespace Kicken\Gearman\Test\Network;

use Kicken\Gearman\Events\EventEmitter;
use Kicken\Gearman\Exception\NotConnectedException;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketBuffer;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Promise\PromiseInterface;
use function React\Promise\resolve;

class PacketPlaybackConnection implements Endpoint {
    use EventEmitter;

    /** @var PacketHandler[] */
    private array $handlerList = [];
    private array $sequence;
    private LoopInterface $loop;
    private PacketBuffer $writeBuffer;
    private array $options = [];
    private string $clientId = 'playback';
    private array $receivedPacketList = [];

    public function __construct(array $packetSequence, LoopInterface $loop = null){
        $this->sequence = $packetSequence;
        $this->loop = $loop ?? Loop::get();
        $this->writeBuffer = new PacketBuffer();
        $this->loop->futureTick(\Closure::fromCallable([$this, 'tick']));
    }

    public function receivedPacket(IncomingPacket $packet){

    }

    public function didReceivePacket(IncomingPacket $packet) : bool{
        return in_array((string)$packet, $this->receivedPacketList, true);
    }

    private function isConnected() : bool{
        return count($this->sequence) > 0 || !$this->writeBuffer->isEmpty();
    }

    public function getAddress() : string{
        return 'localhost';
    }

    public function getFd() : int{
        return 0;
    }

    public function writePacket(Packet $packet) : void{
        if (!$this->isConnected()){
            throw new NotConnectedException();
        }
        $this->receivedPacketList[] = (string)$packet;
        $this->writeBuffer->feed($packet);
    }

    public function disconnect() : void{
    }

    public function addPacketHandler(PacketHandler $handler) : void{
        $this->handlerList[] = $handler;
    }

    public function removePacketHandler(PacketHandler $handler) : void{
        $index = array_search($handler, $this->handlerList, true);
        if ($index !== false){
            unset($this->handlerList[$index]);
        }

        if (!$this->handlerList){
            $this->disconnect();
        }
    }

    public function connect() : PromiseInterface{
        return resolve($this);
    }

    public function listen(callable $handler) : void{
    }

    public function shutdown() : void{
    }

    public function playback(){
        $this->loop->run();
    }

    public function setClientId(string $clientId){
        $this->clientId = $clientId;
    }

    public function getClientId() : string{
        return $this->clientId;
    }

    public function setOption(string $option) : bool{
        $this->options[] = $option;

        return true;
    }

    private function tick(){
        $packet = array_shift($this->sequence);
        if ($packet instanceof OutgoingPacket){
            $this->emitPacket($packet);
        } else if ($packet instanceof IncomingPacket){
            $lastPacketWritten = $this->writeBuffer->readPacket();
            if (!$lastPacketWritten){
                throw new \RuntimeException('No packet written when one was expected.');
            } else if ((string)$lastPacketWritten !== (string)$packet){
                throw $this->unexpectedPacket($lastPacketWritten, $packet);
            }
        } else {
            throw new \RuntimeException('Playback sequence must consist of OutgoingPacket or IncomingPacket elements only.');
        }

        if ($this->sequence){
            $this->loop->futureTick(\Closure::fromCallable([$this, 'tick']));
        }
    }

    private function emitPacket(Packet $packet){
        $handlerList = $this->handlerList;
        do {
            $handler = array_shift($handlerList);
        } while ($handler && !$handler->handlePacket($this, $packet));
    }

    private function unexpectedPacket(Packet $expected, BinaryPacket $actual) : \RuntimeException{
        $message = sprintf('Unexpected packet written.  Expected: %s, Actual: %s'
            , $expected instanceof BinaryPacket ? $this->encodePacket($expected) : ''
            , $this->encodePacket($actual)
        );

        return new \RuntimeException($message);
    }

    private function encodePacket(BinaryPacket $packet){
        return json_encode([
            'magic' => PacketMagic::toReadableString($packet->getMagic())
            , 'type' => PacketType::toReadableString($packet->getType())
            , 'arguments' => $packet->getArgumentList()
        ]);
    }
}
