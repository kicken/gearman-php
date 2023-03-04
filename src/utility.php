<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\GearmanEndpoint;
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
 * @param string|string[]|Endpoint|Endpoint[] $serverList
 * @param ?LoopInterface $loop
 *
 * @return Endpoint[]
 */
function mapToEndpointObjects($serverList, ?LoopInterface $loop) : array{
    if (!is_array($serverList)){
        $serverList = [$serverList];
    }

    return array_map(function($item) use ($loop){
        if (is_string($item)){
            return new GearmanEndpoint($item, null, $loop);
        } else if ($item instanceof Endpoint){
            return $item;
        }
        throw new \InvalidArgumentException();
    }, $serverList);
}
