<?php

namespace Kicken\Gearman\Job;

use Kicken\Gearman\Client;
use Kicken\Gearman\Job\Data\ClientJobData;
use React\Promise\PromiseInterface;
use function React\Promise\reject;

class ClientBackgroundJob extends ClientJob {
    private ?Client $client;

    public function __construct(ClientJobData $jobDetails, Client $client = null){
        parent::__construct($jobDetails);
        $this->client = $client;
    }

    public function getStatus() : PromiseInterface{
        if ($this->client){
            return $this->client->getJobStatus($this->getJobHandle());
        } else {
            return reject('No client');
        }
    }
}
