<?php

namespace Kicken\Gearman\Test\Server\JobQueue;

use Kicken\Gearman\Exception\DirectoryDoesNotExistException;
use Kicken\Gearman\Job\JobPriority;
use Kicken\Gearman\Server\JobQueue\FilesystemJobQueue;
use Kicken\Gearman\Server\ServerJobData;
use PHPUnit\Framework\TestCase;

class FilesystemJobQueueTest extends TestCase {
    private string $spoolDir;

    public function setUp() : void{
        do {
            $this->spoolDir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . bin2hex(random_bytes(4));
        } while (file_exists($this->spoolDir));
        mkdir($this->spoolDir);
    }

    public function tearDown() : void{
        foreach (new \DirectoryIterator($this->spoolDir) as $item){
            if ($item->isFile()){
                unlink($item->getPathname());
            }
        }
        rmdir($this->spoolDir);
    }

    public function testExceptionOnNonExistentDirectory(){
        $this->expectException(DirectoryDoesNotExistException::class);
        new FilesystemJobQueue('missing-test-spool-dir');
    }

    public function testEnqueuePersists(){
        $queue = new FilesystemJobQueue($this->spoolDir);
        $queue->enqueue($this->createTestJob());
        $this->assertFileCount(1, $this->spoolDir);
    }

    public function testDequeueDeletes(){
        $queue = new FilesystemJobQueue($this->spoolDir);
        $queue->enqueue($this->createTestJob());
        $this->assertFileCount(1, $this->spoolDir);
        $queue->dequeue(['test']);
        $this->assertFileCount(0, $this->spoolDir);
    }

    public function testRestoresOnConstruct(){
        $queue = new FilesystemJobQueue($this->spoolDir);
        $queue->enqueue($this->createTestJob());
        $queue->enqueue($this->createTestJob());
        $queue->enqueue($this->createTestJob());

        $queue = new FilesystemJobQueue($this->spoolDir);
        $jobCount = 0;
        while ($queue->dequeue(['test'])){
            $jobCount++;
        }
        $this->assertEquals(3, $jobCount);
    }

    private function assertFileCount(int $expected, string $dir){
        $actual = 0;
        foreach (new \DirectoryIterator($dir) as $item){
            if ($item->isFile()){
                $actual++;
            }
        }
        $this->assertEquals($expected, $actual);
    }

    private function createTestJob() : ServerJobData{
        static $counter = 1;

        return new ServerJobData(
            'H:test:' . ($counter++),
            'test',
            '1234',
            'test',
            JobPriority::NORMAL,
            false,
            new \DateTimeImmutable()
        );
    }
}
