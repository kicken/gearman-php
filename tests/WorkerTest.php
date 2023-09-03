<?php

namespace Kicken\Gearman\Test;

use Kicken\Gearman\Client;
use Kicken\Gearman\Network\Endpoint;
use Kicken\Gearman\Network\GearmanEndpoint;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Test\Network\IncomingPacket;
use Kicken\Gearman\Test\Network\OutgoingPacket;
use Kicken\Gearman\Test\Network\PacketPlaybackConnection;
use Kicken\Gearman\Worker\WorkerJob;
use PHPUnit\Framework\TestCase;
use function React\Promise\resolve;

class WorkerTest extends TestCase {
    public function testWorkWithImmediateJob(){
        $worker = $this->createWorker(new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::CAN_DO, ['reverse'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, ['H:test:1', 'reverse', '', 'test'])
        ]));

        $mockWorkerFunction = $this->getMockBuilder(\stdClass::class)->addMethods(['reverse'])->getMock();
        $mockWorkerFunction->expects($this->once())
            ->method('reverse')
            ->with($this->isInstanceOf(WorkerJob::class))
            ->willReturnCallback(function(WorkerJob $job) use ($worker){
                $worker->stopWorking();
                $this->assertEquals('H:test:1', $job->getJobHandle());
                $this->assertEquals('reverse', $job->getFunction());
                $this->assertEquals('test', $job->getWorkload());
            });

        $worker->registerFunction('reverse', [$mockWorkerFunction, 'reverse'])->work();
    }

    public function testWorkWithDelayedJob(){
        $worker = $this->createWorker(new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::CAN_DO, ['reverse'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::NO_JOB)
            , new IncomingPacket(PacketMagic::REQ, PacketType::PRE_SLEEP)
            , new OutgoingPacket(PacketMagic::REQ, PacketType::NOOP)
            , new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, ['H:test:1', 'reverse', '', 'test'])
        ]));

        $mockWorkerFunction = $this->getMockBuilder(\stdClass::class)->addMethods(['reverse'])->getMock();
        $mockWorkerFunction->expects($this->once())
            ->method('reverse')
            ->with($this->isInstanceOf(WorkerJob::class))
            ->willReturnCallback(function(WorkerJob $job) use ($worker){
                $worker->stopWorking();
                $this->assertEquals('H:test:1', $job->getJobHandle());
                $this->assertEquals('reverse', $job->getFunction());
                $this->assertEquals('test', $job->getWorkload());
            });

        $worker->registerFunction('reverse', [$mockWorkerFunction, 'reverse'])->work();
    }

    public function testWorkMultipleTimes(){
        $worker = $this->createWorker(new PacketPlaybackConnection([
            new IncomingPacket(PacketMagic::REQ, PacketType::CAN_DO, ['reverse'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, ['H:test:1', 'reverse', '', 'test'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::WORK_COMPLETE, ['H:test:1', 'tset'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, ['H:test:2', 'reverse', '', 'test'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::WORK_COMPLETE, ['H:test:2', 'tset'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, ['H:test:3', 'reverse', '', 'test'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::WORK_COMPLETE, ['H:test:3', 'tset'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, ['H:test:4', 'reverse', '', 'test'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::WORK_COMPLETE, ['H:test:4', 'tset'])
        ]));

        /*$mockWorkerFunction = $this->getMockBuilder(\stdClass::class)->addMethods(['reverse'])->getMock();
        $mockWorkerFunction->expects($this->once())
            ->method('reverse')
            ->with($this->isInstanceOf(WorkerJob::class))
            ->willReturnCallback(function(WorkerJob $job) use ($worker){
                $this->assertMatchesRegularExpression('/H:test:\d+/', $job->getJobHandle());
                $this->assertEquals('reverse', $job->getFunction());
                $this->assertEquals('test', $job->getWorkload());
                if ($job->getJobHandle() === 'H:test:4'){
                    $worker->stopWorking();
                }

                return strrev($job->getWorkload());
            });*/

        $worker->registerFunction('reverse', function(WorkerJob $job){
            return strrev($job->getWorkload());
        })->work();
    }

    private function createWorker(Endpoint $connection) : Client{
        $endpoint = $this->getMockBuilder(GearmanEndpoint::class)->disableOriginalConstructor()->getMock();
        $endpoint->expects($this->atLeastOnce())->method('connect')->willReturn(resolve($connection));

        return new Client($endpoint);
    }
}
