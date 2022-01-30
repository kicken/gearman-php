<?php

namespace Kicken\Gearman\Job\Data;

class ClientJobData extends JobData {
    public string $function;
    public string $workload;
    public string $unique;
    public int $priority;
    public bool $background = false;
    public bool $finished = false;
    public ?string $result = null;
    public ?string $data = null;
    /** @var callable[] */
    public array $callbacks = [];


    public function __construct(string $function, string $workload, string $unique, int $priority){
        parent::__construct(null);
        $this->function = $function;
        $this->workload = $workload;
        $this->unique = $unique;
        $this->priority = $priority;
    }

    public function addCallback($type, callable $fn){
        $this->callbacks[$type][] = $fn;
    }

    public function triggerCallback($type){
        if (isset($this->callbacks[$type])){
            foreach ($this->callbacks[$type] as $fn){
                call_user_func($fn);
            }
        }
    }
}