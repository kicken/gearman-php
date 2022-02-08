<?php

namespace Kicken\Gearman\Protocol;

use Kicken\Gearman\Exception\InsufficientDataException;
use function Kicken\Gearman\fromBigEndian;

class PacketBuffer {
    private string $buffer = '';
    private int $bufferLength = 0;
    private int $readOffset = 0;

    public function feed(string $data){
        $this->buffer .= $data;
    }

    public function readPacket() : ?Packet{
        try {
            return $this->extractPacket();
        } catch (InsufficientDataException $ex){
            return null;
        }
    }

    private function extractPacket() : Packet{
        $this->begin();
        $first = $this->extractBytes(1);
        if ($first === '\0'){
            return $this->extractBinaryPacket();
        } else {
            return $this->extractAdministrativePacket();
        }
    }

    private function extractBinaryPacket() : BinaryPacket{
        $magic = $this->extractBytes(4);
        $type = $this->extractBytes(4);
        $type = fromBigEndian($type);
        $size = $this->extractBytes(4);
        $size = fromBigEndian($size);
        $arguments = $this->extractBytes($size);

        $arguments = explode(chr(0), $arguments);
        $packet = new BinaryPacket($magic, $type, $arguments);
        $this->commit();

        return $packet;
    }

    private function extractAdministrativePacket() : AdministrativePacket{
        $this->begin();
        $command = $this->extractLine();
        $packet = new AdministrativePacket($command);
        $this->commit();

        return $packet;
    }

    private function begin(){
        $this->readOffset = 0;
        $this->bufferLength = strlen($this->buffer);
    }

    private function commit(){
        if ($this->readOffset === $this->bufferLength){
            $this->buffer = '';
        } else {
            $this->buffer = substr($this->buffer, $this->readOffset);
            $this->bufferLength = strlen($this->buffer);
        }
    }

    private function extractBytes(int $byteCount){
        if ($this->bufferLength < $this->readOffset + $byteCount){
            throw new InsufficientDataException();
        }

        $bytes = substr($this->buffer, $this->readOffset, $byteCount);
        $this->readOffset += $byteCount;

        return $bytes;
    }

    private function extractLine(){
        $pos = strpos($this->buffer, "\n", $this->readOffset);
        if ($pos === false){
            $pos = strpos($this->buffer, "\r", $this->readOffset);
            if ($pos === false){
                throw new InsufficientDataException();
            }
        }

        $length = ($pos + 1) - $this->readOffset;
        $bytes = substr($this->buffer, $this->readOffset, $length);
        $this->readOffset += $length;

        return $bytes;
    }
}
