<?php

namespace Kicken\Gearman;

use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\GearmanEndpoint;

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
 * @param ServiceContainer $services
 *
 * @return Endpoint[]
 */
function mapToEndpointObjects($serverList, ServiceContainer $services) : array{
    if (!is_array($serverList)){
        $serverList = [$serverList];
    }

    return array_map(function($item) use ($services){
        if (is_string($item)){
            return new GearmanEndpoint($item, null, $services);
        } else if ($item instanceof Endpoint){
            return $item;
        }
        throw new \InvalidArgumentException();
    }, $serverList);
}

function normalizeFunctionName(string $function) : string{
    return strtolower(trim($function));
}
