<?php

namespace Kicken\Gearman\Test\Network\PacketHandler;

use Kicken\Gearman\Client\BackgroundJob;
use Kicken\Gearman\Client\ClientJobData;
use Kicken\Gearman\Client\ForegroundJob;
use Kicken\Gearman\Client\PacketHandler\CreateJobHandler;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Protocol\PacketMagic;
use Kicken\Gearman\Protocol\PacketType;
use Kicken\Gearman\Test\Network\OutgoingPacket;
use Kicken\Gearman\Test\Network\PacketPlaybackConnection;
use PHPUnit\Framework\TestCase;

class CreateJobHandlerTest extends TestCase {
    public function testJobHandleIsReceived(){
        $data = new ClientJobData('reverse', 'test', '', JobPriority::NORMAL);
        $job = new ForegroundJob($data);
        $handler = new CreateJobHandler($data);

        $server = new PacketPlaybackConnection([
            new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
        ]);
        $server->addPacketHandler($handler);
        $server->playback();

        $this->assertEquals('H:test:1', $job->getJobHandle());
    }

    public function testHandlerRemovedAfterJobCreatedForBackgroundJobs(){
        $data = new ClientJobData('reverse', 'test', '', JobPriority::NORMAL);
        $data->background = true;
        $job = new BackgroundJob($data, null);
        $handler = new CreateJobHandler($data);

        $server = new PacketPlaybackConnection([
            new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
        ]);
        $server->addPacketHandler($handler);
        $server->playback();

        $this->assertEquals('H:test:1', $job->getJobHandle());
    }

    public function testForegroundJobStatusCallbackTriggered(){
        /**
         * @var ForegroundJob $job
         * @var CreateJobHandler $handler
         * @var PacketPlaybackConnection $server
         */
        [$job, , $server] = $this->setupCallbackTest([
            new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_STATUS, ['H:test:1', 1, 10])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_STATUS, ['H:test:1', 2, 10])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_STATUS, ['H:test:1', 3, 10])
        ]);

        $numerator = $denominator = 0;
        $job->onStatus(function() use (&$numerator, &$denominator, $job){
            $numerator += $job->getNumerator();
            $denominator += $job->getDenominator();
        });
        $server->playback();

        $this->assertEquals('H:test:1', $job->getJobHandle());
        $this->assertEquals(6, $numerator);
        $this->assertEquals(30, $denominator);
    }

    public function testForegroundJobDataCallbackTriggered(){
        /**
         * @var ForegroundJob $job
         * @var CreateJobHandler $handler
         * @var PacketPlaybackConnection $server
         */
        [$job, $handler, $server] = $this->setupCallbackTest([
            new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_DATA, ['H:test:1', 'Ready...'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_DATA, ['H:test:1', 'Set...'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_DATA, ['H:test:1', 'Go!'])
        ]);

        $data = '';
        $job->onData(function() use (&$data, $job){
            $data .= $job->getData();
        });
        $server->playback();

        $this->assertEquals('H:test:1', $job->getJobHandle());
        $this->assertEquals('Ready...Set...Go!', $data);
        $this->assertEquals('Go!', $job->getData());
    }

    public function testForegroundJobWarningCallbackTriggered(){
        /**
         * @var ForegroundJob $job
         * @var CreateJobHandler $handler
         * @var PacketPlaybackConnection $server
         */
        [$job, $handler, $server] = $this->setupCallbackTest([
            new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_WARNING, ['H:test:1', 'Warning!'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_WARNING, ['H:test:1', 'Warning!'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_WARNING, ['H:test:1', 'Warning!'])
        ]);

        $data = '';
        $job->onWarning(function() use (&$data, $job){
            $data .= $job->getData();
        });
        $server->playback();

        $this->assertEquals('H:test:1', $job->getJobHandle());
        $this->assertEquals('Warning!Warning!Warning!', $data);
        $this->assertEquals('Warning!', $job->getData());
    }

    public function testForegroundJobCompleteCallbackTriggered(){
        /**
         * @var ForegroundJob $job
         * @var CreateJobHandler $handler
         * @var PacketPlaybackConnection $server
         */
        [$job, $handler, $server] = $this->setupCallbackTest([
            new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_STATUS, ['H:test:1', 10, 10])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_DATA, ['H:test:1', 'Finished'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_COMPLETE, ['H:test:1', 'tset'])
        ]);

        $complete = false;
        $job->onComplete(function() use (&$complete){
            $complete = true;
        });
        $server->playback();

        $this->assertEquals('H:test:1', $job->getJobHandle());
        $this->assertTrue($complete);
        $this->assertEquals(10, $job->getNumerator());
        $this->assertEquals(10, $job->getDenominator());
        $this->assertEquals('Finished', $job->getData());
        $this->assertEquals('tset', $job->getResult());
    }

    public function testForegroundJobFailCallbackTriggered(){
        /**
         * @var ForegroundJob $job
         * @var CreateJobHandler $handler
         * @var PacketPlaybackConnection $server
         */
        [$job, $handler, $server] = $this->setupCallbackTest([
            new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_FAIL, ['H:test:1'])
        ]);

        $failure = false;
        $job->onFail(function() use (&$failure){
            $failure = true;
        });
        $server->playback();

        $this->assertEquals('H:test:1', $job->getJobHandle());
        $this->assertTrue($failure);
    }

    public function testForegroundJobExceptionCallbackTriggered(){
        /**
         * @var ForegroundJob $job
         * @var CreateJobHandler $handler
         * @var PacketPlaybackConnection $server
         */
        [$job, $handler, $server] = $this->setupCallbackTest([
            new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_EXCEPTION, ['H:test:1', 'Error'])
        ]);

        $failure = false;
        $job->onException(function() use (&$failure){
            $failure = true;
        });
        $server->playback();

        $this->assertEquals('H:test:1', $job->getJobHandle());
        $this->assertTrue($failure);
        $this->assertEquals('Error', $job->getData());
    }

    public function testOnlyHandlesOwnJobs(){
        /**
         * @var ForegroundJob $job
         * @var CreateJobHandler $handler
         * @var PacketPlaybackConnection $server
         */
        [$job, $handler, $server] = $this->setupCallbackTest([
            new OutgoingPacket(PacketMagic::RES, PacketType::JOB_CREATED, ['H:test:1'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_STATUS, ['H:test:1', 1, 10])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_STATUS, ['H:test:2', 9, 10])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_STATUS, ['H:test:1', 2, 10])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_STATUS, ['H:test:2', 10, 10])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_DATA, ['H:test:1', 'Finished'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_DATA, ['H:test:2', 'Finished'])
            , new OutgoingPacket(PacketMagic::RES, PacketType::WORK_COMPLETE, ['H:test:2', 'tset'])
        ]);

        $counter = 0;
        $job->onStatus(function() use (&$counter){
            $counter++;
        });
        $complete = false;
        $job->onComplete(function() use (&$complete){
            $complete = true;
        });

        $server->playback();

        $this->assertEquals('H:test:1', $job->getJobHandle());
        $this->assertEquals(2, $counter);
        $this->assertEquals(2, $job->getNumerator());
        $this->assertEquals(10, $job->getDenominator());
        $this->assertFalse($complete);
    }

    private function setupCallbackTest(array $packetSequence) : array{
        $data = new ClientJobData('reverse', 'test', '', JobPriority::NORMAL);
        $job = new ForegroundJob($data);
        $handler = new CreateJobHandler($data);
        $server = new PacketPlaybackConnection($packetSequence);
        $server->addPacketHandler($handler);

        return [$job, $handler, $server];
    }
}
