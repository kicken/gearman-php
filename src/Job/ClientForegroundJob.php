<?php

namespace Kicken\Gearman\Job;

class ClientForegroundJob extends ClientJob {
    public function onStatus(callable $fn) : self{
        $this->addCallback('status', $fn);

        return $this;
    }

    public function onData(callable $fn) : self{
        $this->addCallback('data', $fn);

        return $this;
    }

    public function onWarning(callable $fn) : self{
        $this->addCallback('warning', $fn);

        return $this;
    }

    public function onComplete(callable $fn) : self{
        $this->addCallback('complete', $fn);

        return $this;
    }

    public function onFail(callable $fn) : self{
        $this->addCallback('fail', $fn);

        return $this;
    }

    public function onException(callable $fn) : self{
        $this->addCallback('exception', $fn);

        return $this;
    }

    private function addCallback(string $type, callable $fn){
        $wrapper = function() use ($fn){
            return call_user_func($fn, $this);
        };

        $this->jobDetails->addCallback($type, $wrapper);
    }
}