<?php
/** @noinspection PhpComposerExtensionStubsInspection */

namespace Kicken\Gearman\Server\JobQueue;

use Kicken\Gearman\Server\ServerJobData;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;

class FilesystemJobQueue extends MemoryJobQueue {
    use LoggerAwareTrait;

    private string $directory;

    public function __construct(string $directory, ?LoggerInterface $logger = null){
        parent::__construct($logger);
        $this->directory = $directory;
        if (!extension_loaded('json')){
            throw new \RuntimeException('JSON extension missing');
        }

        if (!file_exists($directory)){
            throw new \RuntimeException('Directory does not exist.');
        }

        $this->restore();
    }

    public function enqueue(ServerJobData $jobData) : void{
        $this->persist($jobData);
        parent::enqueue($jobData);
    }

    public function dequeue(array $functionList) : ?ServerJobData{
        $jobData = parent::dequeue($functionList);
        if ($jobData){
            $this->remove($jobData);
        }

        return $jobData;
    }

    private function restore() : void{
        $iterator = new \DirectoryIterator($this->directory);
        foreach ($iterator as $file){
            if (!$file->isFile()){
                continue;
            }

            $details = file_get_contents($file->getPathname());
            $details = json_decode($details);
            if (json_last_error() !== JSON_ERROR_NONE){
                $this->logger->warning('Unable to parse file ', [
                    'file' => $file->getPathname()
                    , 'error' => json_last_error()
                    , 'message' => json_last_error_msg()
                ]);
            } else {
                $this->enqueue($this->constructJobData($details));
            }
        }
    }

    private function persist(ServerJobData $jobData) : void{
        $fullPath = $this->getFilePath($jobData);
        file_put_contents($fullPath, json_encode([
            'handle' => $jobData->jobHandle
            , 'function' => $jobData->function
            , 'workload' => $jobData->workload
            , 'uniqueId' => $jobData->uniqueId
            , 'priority' => $jobData->priority
            , 'background' => $jobData->background
            , 'createdOn' => $jobData->created->format('r')
        ]));
    }

    private function remove(ServerJobData $jobData) : void{
        $fullPath = $this->getFilePath($jobData);
        if (file_exists($fullPath)){
            unlink($fullPath);
        }
    }

    private function getFilePath(ServerJobData $jobData) : string{
        $fileName = sha1($jobData->jobHandle);
        $fullPath = $this->directory . '/' . $fileName;

        return $fullPath;
    }

    private function constructJobData(\stdClass $details) : ServerJobData{
        return new ServerJobData(
            $details->handle,
            $details->function,
            $details->uniqueId,
            $details->workload,
            $details->priority,
            $details->background,
            new \DateTimeImmutable($details->createdOn)
        );
    }
}
