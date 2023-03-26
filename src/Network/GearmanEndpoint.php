<?php

namespace Kicken\Gearman\Network;

use Kicken\Gearman\Events\EndpointEvents;
use Kicken\Gearman\Events\EventEmitter;
use Kicken\Gearman\Exception\CouldNotConnectException;
use Kicken\Gearman\Exception\LostConnectionException;
use Kicken\Gearman\Network\PacketHandler\PacketHandler;
use Kicken\Gearman\Protocol\Packet;
use Kicken\Gearman\Protocol\PacketBuffer;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use function React\Promise\reject;
use function React\Promise\resolve;

class GearmanEndpoint implements Endpoint {
    use LoggerAwareTrait;
    use EventEmitter;

    private LoopInterface $loop;

    private string $url;
    private int $connectTimeout;
    private bool $autoDisconnect = true;
    private bool $connectedEventTriggered = false;

    /** @var resource */
    private $stream = null;
    private ?Deferred $connectingDeferred = null;
    private ?PromiseInterface $connectingPromise = null;
    private ?TimerInterface $timeoutTimer = null;

    private string $writeBuffer = '';
    private PacketBuffer $readBuffer;
    private array $packetHandlerList = [];

    public function __construct(string $url, int $connectTimeout = null, LoopInterface $loop = null){
        $this->url = $url;
        $this->connectTimeout = $connectTimeout ?? ini_get('default_socket_timeout');
        $this->loop = $loop ?? Loop::get();
        $this->logger = new NullLogger();
        $this->readBuffer = new PacketBuffer();
    }

    public function connect(bool $autoDisconnect) : PromiseInterface{
        $this->autoDisconnect = $autoDisconnect;
        if ($this->connectingPromise){
            return $this->connectingPromise = $this->connectingPromise->then();
        } else if ($this->isConnected()){
            return resolve($this);
        }

        $this->connectedEventTriggered = false;
        $this->stream = stream_socket_client($this->url, $errno, $errStr, null, STREAM_CLIENT_ASYNC_CONNECT);
        if (!$this->stream){
            return reject(new CouldNotConnectException($this, $errno, $errStr));
        }

        $this->connectingDeferred = new Deferred();
        $this->timeoutTimer = $this->loop->addTimer($this->connectTimeout, function(){
            $this->completeConnectionAttempt();
        });

        $this->loop->addWriteStream($this->stream, function(){
            $this->completeConnectionAttempt();
        });

        return $this->connectingPromise = $this->connectingDeferred->promise();
    }

    public function isConnected() : bool{
        $remoteAddr = $this->stream ? stream_socket_get_name($this->stream, true) : null;

        return $this->stream !== null && $remoteAddr;
    }

    public function disconnect() : void{
        if ($this->stream){
            $this->loop->removeReadStream($this->stream);
            $this->loop->removeWriteStream($this->stream);
            fclose($this->stream);
            $this->stream = null;
            if ($this->connectingDeferred){
                $this->connectingDeferred->reject(new CouldNotConnectException($this));
                $this->connectingDeferred = null;
                $this->connectingPromise = null;
            }
            if ($this->timeoutTimer){
                $this->loop->cancelTimer($this->timeoutTimer);
                $this->timeoutTimer = null;
            }
            if ($this->connectedEventTriggered){
                $this->emit(EndpointEvents::DISCONNECTED, $this);
            }
        }
    }

    public function getFd() : int{
        return (int)$this->stream;
    }

    public function getAddress() : string{
        return $this->url;
    }

    public function listen(callable $handler){
        $this->stream = stream_socket_server($this->url, $errNo, $errStr);
        if (!$this->stream){
            throw new CouldNotConnectException($this, $errNo, $errStr);
        }

        $this->loop->addReadStream($this->stream, function() use ($handler){
            $this->accept($handler);
        });
    }

    public function addPacketHandler(PacketHandler $handler) : void{
        $this->packetHandlerList[] = $handler;
    }

    public function removePacketHandler(PacketHandler $handler) : void{
        $key = array_search($handler, $this->packetHandlerList, true);
        if ($key !== false){
            unset($this->packetHandlerList[$key]);
            if (!$this->packetHandlerList && $this->autoDisconnect){
                $this->disconnect();
            }
        }
    }

    public function writePacket(Packet $packet) : void{
        if ($this->isConnected()){
            $this->writeBuffer .= $packet;
            $this->flush();
        }
    }

    private function flush() : void{
        set_error_handler(function($errNo){
            if ($errNo == E_NOTICE){
                throw new LostConnectionException();
            }
        });
        $written = fwrite($this->stream, $this->writeBuffer);
        restore_error_handler();
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

    private function accept(callable $handler){
        $connection = stream_socket_accept($this->stream);
        if ($connection){
            stream_set_blocking($connection, false);
            $remote = stream_socket_get_name($connection, true);
            $endpoint = new self($remote, $this->connectTimeout, $this->loop);
            $endpoint->setLogger($this->logger);
            $endpoint->stream = $connection;
            $endpoint->setupStream();
            call_user_func($handler, $endpoint);
        }
    }

    private function setupStream(){
        $this->logger->info('Successfully connected to server', ['endpoint' => $this->url]);
        stream_set_blocking($this->stream, false);
        $this->loop->addReadStream($this->stream, function(){
            $this->buffer();
            $this->emitPackets();
        });
        $this->emit(EndpointEvents::CONNECTED, $this);
        $this->connectedEventTriggered = true;
    }

    private function completeConnectionAttempt() : void{
        $this->loop->cancelTimer($this->timeoutTimer);
        $this->loop->removeWriteStream($this->stream);
        if ($this->isConnected()){
            $this->setupStream();
            $this->connectingDeferred->resolve($this);
        } else {
            $this->stream = null;
            $error = new CouldNotConnectException($this);
            $this->logger->warning($error->getMessage(), ['url' => $this->url]);
            $this->connectingDeferred->reject($error);
        }
        $this->connectingPromise = null;
        $this->connectingDeferred = null;
        $this->timeoutTimer = null;
    }

    public function shutdown(){
        $this->loop->removeWriteStream($this->stream);
        $this->loop->removeReadStream($this->stream);
        stream_socket_shutdown($this->stream, STREAM_SHUT_RDWR);
        fclose($this->stream);
        $this->stream = null;
    }

    private function buffer() : void{
        do {
            $data = fread($this->stream, 8192);
            if ($data){
                $this->readBuffer->feed($data);
            }
        } while ($data);

        if (feof($this->stream)){
            $this->disconnect();
        }
    }

    private function emitPackets() : void{
        try {
            while ($packet = $this->readBuffer->readPacket()){
                $handlerQueue = $this->packetHandlerList;
                $handled = false;
                do {
                    $handler = array_shift($handlerQueue);
                } while ($handler && !($handled = $handler->handlePacket($this, $packet)));

                if (!$handled){
                    $this->logger->warning('Unhandled Packet ' . get_class($packet), ['packet' => $this->encodePacket($packet)]);
                }
            }
        } catch (LostConnectionException $ex){
            $this->disconnect();
        }
    }

    private function encodePacket(string $packet){
        return preg_replace_callback('/[^A-Za-z0-9]/', function($v){
            return '\x' . bin2hex($v[0]);
        }, $packet);
    }
}
