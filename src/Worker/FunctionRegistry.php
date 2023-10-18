<?php

namespace Kicken\Gearman\Worker;

use Kicken\Gearman\Events\FunctionRegistered;
use Kicken\Gearman\Events\FunctionUnregistered;
use Kicken\Gearman\Exception\LostConnectionException;
use Kicken\Gearman\Exception\NoRegisteredFunctionException;
use Kicken\Gearman\ServiceContainer;

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
    }

    public function unregister(WorkerFunction $fn){
        unset($this->functionList[$this->normalize($fn->name)]);
        $this->services->eventDispatcher->dispatch(new FunctionUnregistered($fn->name));
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
            $fn = $this->functionList[$normalizedFn];
            $result = call_user_func($fn->callback, $job);
            if ($result === false){
                $job->sendFail();
            } else {
                $job->sendComplete((string)$result);
            }
        } catch (LostConnectionException $e){
            throw $e;
        } catch (\Exception $e){
            $job->sendException(get_class($e) . ': ' . $e->getMessage());
        }
    }

    private function normalize(string $name) : string{
        return strtolower(trim($name));
    }
}
