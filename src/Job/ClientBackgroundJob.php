<?php

namespace Kicken\Gearman\Job;

use Kicken\Gearman\Client;
use Kicken\Gearman\Job\Data\ClientJobData;
use React\Promise\PromiseInterface;

class ClientBackgroundJob extends ClientJob {
    private Client $client;

    public function __construct(Client $client, ClientJobData $jobDetails){
        parent::__construct($jobDetails);
        $this->client = $client;
    }

    public function getStatus() : PromiseInterface{
        return $this->client->getJobStatus($this->getJobHandle());
    }
}
