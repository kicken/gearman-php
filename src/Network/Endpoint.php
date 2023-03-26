<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use React\Promise\PromiseInterface;

interface Endpoint {
    public function connect(bool $autoDisconnect) : PromiseInterface;

    public function disconnect() : void;

    public function getFd() : int;

    public function getAddress() : string;

    public function listen(callable $handler);

    public function shutdown();

    public function on(string $event, callable $callback) : void;

    public function addPacketHandler(PacketHandler $handler);

    public function removePacketHandler(PacketHandler $handler);
}
