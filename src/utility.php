<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Network\Server;
use React\EventLoop\LoopInterface;

/**
 * Convert an integer from machine to big endian format
 *
 * @param int $num
 *
 * @return string
 */
function toBigEndian(int $num) : string{
    return pack('N', $num);
}

/**
 * Convert an integer from big endian format to machine format.
 *
 * @param string $data
 *
 * @return int
 */
function fromBigEndian(string $data) : int{
    $data = unpack('Nnum', $data);

    return $data['num'];
}

/**
 * @param string|string[]|Server|Server[] $serverList
 *
 * @return Server[]
 */
function mapToServerObjects($serverList, LoopInterface $loop) : array{
    if (!is_array($serverList)){
        $serverList = [$serverList];
    }

    return array_map(function($item) use ($loop){
        if (is_string($item)){
            return new Server($item, null, $loop);
        } else if ($item instanceof Server){
            return $item;
        }
        throw new \InvalidArgumentException();
    }, $serverList);
}
