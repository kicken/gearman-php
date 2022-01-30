<?php

namespace Kicken\Gearman;

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
