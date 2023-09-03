<?php

namespace Kicken\Gearman\Client;

use Kicken\Gearman\Client;
use React\Promise\PromiseInterface;
use function React\Promise\reject;

class BackgroundJob extends ClientJob {
    private ?Client $client;

    public function __construct(ClientJobData $jobDetails, Client $client = null){
        parent::__construct($jobDetails);
        $this->client = $client;
    }

    public function getStatus() : PromiseInterface{
        if ($this->client){
            return $this->client->getJobStatusAsync($this->getJobHandle());
        } else {
            return reject('No client');
        }
    }
}
