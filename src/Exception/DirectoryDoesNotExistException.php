<?php

namespace Kicken\Gearman\Exception;

class DirectoryDoesNotExistException extends \RuntimeException implements GearmanException {
    public function __construct(string $directory){
        parent::__construct('Directory does not exist: ' . $directory);
    }
}