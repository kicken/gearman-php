<?php
/**
 * Copyright (c) 2015 Keith
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

namespace Kicken\Gearman\Protocol;


use function Kicken\Gearman\toBigEndian;

/**
 * Represents a packet of data received from or sent to the gearman server.
 *
 * @package Kicken\Gearman\Protocol
 */
class Packet {
    private string $magic;
    private int $type;
    private array $arguments;

    /**
     * Construct a new packet of data.
     *
     * @param string $magic The magic code for the packet
     * @param int $type The type of packet to send
     * @param array $arguments Arguments to include with the packet
     */
    public function __construct(string $magic, int $type, array $arguments = []){
        $this->magic = $magic;
        $this->type = $type;
        $this->arguments = $arguments;
    }

    public function getMagic() : string{
        return $this->magic;
    }

    /**
     * Get the packet type.
     *
     * @return int
     */
    public function getType() : int{
        return $this->type;
    }

    /**
     * Get the argument data at the specified index.
     *
     * @param int $index Which argument to get
     *
     * @return mixed
     */
    public function getArgument(int $index){
        if (isset($this->arguments[$index])){
            return $this->arguments[$index];
        }

        throw new \RangeException('Argument index out of range.');
    }

    public function getArgumentList() : array{
        return $this->arguments;
    }

    /**
     * Compile a packet into a binary string to be sent across the connection.
     *
     * @return string
     */
    public function __toString(){
        $argumentData = implode(chr(0), $this->arguments);
        $size = strlen($argumentData);

        $data = $this->magic
            . toBigEndian($this->type)
            . toBigEndian($size)
            . $argumentData;

        return $data;
    }
}
