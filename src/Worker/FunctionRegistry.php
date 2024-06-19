<?php

namespace Kicken\Gearman\Worker;

use Kicken\Gearman\Events\FunctionRegistered;
use Kicken\Gearman\Events\FunctionUnregistered;
use Kicken\Gearman\Exception\LostConnectionException;
use Kicken\Gearman\Exception\NoRegisteredFunctionException;
use Kicken\Gearman\ServiceContainer;
use Psr\Log\LogLevel;

class FunctionRegistry implements \Countable {
    private ServiceContainer $services;

    /** @var WorkerFunction[] */
    private array $functionList = [];

    public function __construct(ServiceContainer $container){
        $this->services = $container;
    }

    public function count() : int{
        return count($this->functionList);
    }

    public function register(WorkerFunction $fn){
        $this->functionList[$this->normalize($fn->name)] = $fn;
        $this->services->eventDispatcher->dispatch(new FunctionRegistered($fn->name));
        $this->log(LogLevel::INFO, 'Registering function', [
            'function' => $fn
        ]);
    }

    public function unregister(WorkerFunction $fn){
        unset($this->functionList[$this->normalize($fn->name)]);
        $this->services->eventDispatcher->dispatch(new FunctionUnregistered($fn->name));
        $this->log(LogLevel::INFO, 'Removing registered function', [
            'function' => $fn
        ]);
    }

    public function isRegistered(string $fn) : bool{
        return isset($this->functionList[$fn]);
    }

    public function run(WorkerJob $job){
        $normalizedFn = $this->normalize($job->getFunction());
        if (!isset($this->functionList[$normalizedFn])){
            throw new NoRegisteredFunctionException;
        }

        try {
            $this->log(LogLevel::INFO, 'Running job', [
                'handle' => $job->getJobHandle(),
                'function' => $job->getFunction()
            ]);
            $fn = $this->functionList[$normalizedFn];
            $result = call_user_func($fn->callback, $job);
            if ($result === false){
                $this->log(LogLevel::WARNING, 'Job returned false, sending job fail event.');
                $job->sendFail();
            } else {
                $this->log(LogLevel::INFO, 'Job completed successfully.');
                $job->sendComplete((string)$result);
            }
        } catch (LostConnectionException $e){
            $this->log(LogLevel::ERROR, 'Lost connection to sever while running job.');
            throw $e;
        } catch (\Exception $e){
            $this->log(LogLevel::ERROR, 'Encountered exception while running job.', [
                'exception' => get_class($e),
                'message' => $e->getMessage(),
            ]);
            $job->sendException(get_class($e) . ': ' . $e->getMessage());
        }
    }

    private function normalize(string $name) : string{
        return strtolower(trim($name));
    }

    private function log(string $level, string $message, array $params = []) : void{
        $this->services->logger->log($level, $message, $params);
    }
}
