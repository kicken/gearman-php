<?php

namespace Kicken\Gearman\Test\Network;

use Kicken\Gearman\Exception\NotConnectedException;
use Kicken\Gearman\Network\Connection;
use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use Kicken\Gearman\Protocol\BinaryPacket;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketBuffer;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;

class PacketPlaybackConnection implements Connection {
    /** @var PacketHandler[] */
    private array $handlerList = [];
    private array $sequence;
    private PacketBuffer $writeBuffer;

    public function __construct(array $packetSequence = []){
        $this->sequence = $packetSequence;
        $this->writeBuffer = new PacketBuffer();
    }

    public function isConnected() : bool{
        return count($this->sequence) > 0 || !$this->writeBuffer->isEmpty();
    }

    public function getRemoteAddress() : string{
        return 'localhost';
    }

    public function getFd() : int{
        return 0;
    }

    public function writePacket(Packet $packet) : void{
        if (!$this->isConnected()){
            throw new NotConnectedException();
        }
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

    public function addDisconnectHandler(callable $handler) : void{
    }

    public function hasHandler(PacketHandler $handler) : bool{
        return in_array($handler, $this->handlerList);
    }

    public function playback(){
        while ($packet = array_shift($this->sequence)){
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
