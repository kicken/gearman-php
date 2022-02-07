<?php

namespace Kicken\Gearman\Test;

use Kicken\Gearman\Job\WorkerJob;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Test\Network\AutoPlaybackServer;
use Kicken\Gearman\Test\Network\IncomingPacket;
use Kicken\Gearman\Test\Network\OutgoingPacket;
use Kicken\Gearman\Worker;
use PHPUnit\Framework\TestCase;

class WorkerTest extends TestCase {
    public function testWorkWithImmediateJob(){
        $server = new AutoPlaybackServer([
            new IncomingPacket(PacketMagic::REQ, PacketType::CAN_DO, ['reverse'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, ['H:test:1', 'reverse', '', 'test'])
        ]);

        $worker = new Worker($server);

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
        $server = new AutoPlaybackServer([
            new IncomingPacket(PacketMagic::REQ, PacketType::CAN_DO, ['reverse'])
            , new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::NO_JOB)
            , new IncomingPacket(PacketMagic::REQ, PacketType::PRE_SLEEP)
            , new OutgoingPacket(PacketMagic::REQ, PacketType::NOOP)
            , new IncomingPacket(PacketMagic::REQ, PacketType::GRAB_JOB_UNIQ)
            , new OutgoingPacket(PacketMagic::RES, PacketType::JOB_ASSIGN_UNIQ, ['H:test:1', 'reverse', '', 'test'])
        ]);

        $worker = new Worker($server);

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
        $server = new AutoPlaybackServer([
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
        ]);

        $worker = new Worker($server);

        $mockWorkerFunction = $this->getMockBuilder(\stdClass::class)->addMethods(['reverse'])->getMock();
        $mockWorkerFunction->expects($this->exactly(4))
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
            });

        $worker->registerFunction('reverse', [$mockWorkerFunction, 'reverse'])->work();
    }
}
