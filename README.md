# Gearman-PHP

This library provides a pure PHP implementation of the gearman protocol for creating clients and workers.  It provides an alternative to the PHP extension which is not always available.

This library is not a replacement for the existing extension.  This library provides an alternative and incompatible API.

## Installation

Simply require the kicken/gearman-php package with composer to install the latest version.

    composer require kicken/gearman-php


## Quick start guide

Below is a quick start guide to setting up workers, submitting jobs, and getting a jobs status.   For full details about the API's available, dig into the source code and have a look around.

### Workers

Creating workers involves registering different callback functions with an instance of the Worker class and then calling the work method to start accepting and performing work.  For example:

    <?php
    
    $worker = new \Kicken\Gearman\Worker('127.0.0.1:4730');
    $worker
        ->registerFunction('rot13', function(\Kicken\Gearman\Job\WorkerJob $job){
            $workload = $job->getWorkload();
            echo "Running rot13 task with workload {$workload}\n";
    
            return str_rot13($workload);
        })
        ->work()
    ;

### Submitting Jobs

Jobs can be submitted to the workers using the Client class and either the submitJob or submitBackgroundJob functions.  For example:

    
    $client = new \Kicken\Gearman\Client('127.0.0.1:4730');
    $job = $client->submitBackgroundJob('rot13', 'Foobar');
    echo $job;

A background job will not be able to provide any data back to the client that submitted the job.  The only information that can be obtained from a background job is status by using the getJobStatus function.

A non-background job is able to provide a result or other data back to the client as it is processed.  The client needs to wait for the job to complete to access this data.  This can be accomplished by using the wait function.

There are two ways of getting information regarding a non-background job.  First, the client can wait for it to complete, then access the information from the job object.  

    $client = new \Kicken\Gearman\Client('127.0.0.1:4730');
    $job = $client->submitJob('rot13', 'Foobar');
    $client->wait();
    
    echo $job->getResult();


Second, the client can register different callbacks which will be executed as information becomes available.

    $client = new \Kicken\Gearman\Client('127.0.0.1:4730');
    $job = $client->submitJob('rot13', 'Foobar');
    $job->onStatus(function(\Kicken\Gearman\Job\ClientJob $job){
        echo $job->getProgressPercentage()."% complete\n";
    })->onComplete(function(\Kicken\Gearman\Job\ClientJob $job){
        echo $job->getResult();
    });
    $client->wait();

### Checking a jobs status

If you save the handle to a background job, you can check it's status using the getJobStatus function to determine when it is complete, and how far along it is (if the worker provides progress information).

    $client = new \Kicken\Gearman\Client('127.0.0.1:4740');
    $status = $client->getJobStatus($jobHandle); //previously saved $jobHandle
    $client->wait();
    
    var_dump($status->isKnown(), $status->isRunning(), $status->getNumerator(), $status->getDenominator());


## Timeouts

Both clients and workers have a configurable timeout setting.  The timeout value only controls network communications and how long the client or worker will wait for data from the server.  By default there is no timeout so clients and workers will wait as long as necessary for the network.

To configure a timeout for both workers and clients call the `setTimeout` method and give it a timeout in milliseconds or a boolean true/false value.  Specifying false will permit the code to wait indefinitely.  Specifying true will use PHP's `default_socket_timeout` ini setting.

