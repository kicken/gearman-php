<?php

namespace Kicken\Gearman\Worker;

use Kicken\Gearman\Events\EventEmitter;
use Kicken\Gearman\Events\WorkerEvents;
use Kicken\Gearman\Exception\LostConnectionException;
use Kicken\Gearman\Exception\NoRegisteredFunctionException;

class FunctionRegistry {
    use EventEmitter;

    /** @var WorkerFunction[] */
    private array $functionList = [];

    public function register(WorkerFunction $fn){
        $this->functionList[$this->normalize($fn->name)] = $fn;
        $this->emit(WorkerEvents::REGISTERED_FUNCTION, $fn);
    }

    public function unregister(WorkerFunction $fn){
        unset($this->functionList[$this->normalize($fn->name)]);
        $this->emit(WorkerEvents::UNREGISTERED_FUNCTION, $fn);
    }

    public function isRegistered(string $fn) : bool{
        return isset($this->functionList[$fn]);
    }

    public function run(WorkerJob $job){
        if (!isset($this->functionList[$job->getFunction()])){
            throw new NoRegisteredFunctionException;
        }

        try {
            $fn = $this->functionList[$job->getFunction()];
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
