<?php
/**
 * Created by PhpStorm.
 * User: Keith
 * Date: 2/19/2017
 * Time: 12:32 PM
 */

namespace Kicken\Gearman\Exception;

class TimeoutException extends \RuntimeException {
    public function __construct(){
        parent::__construct("Timeout occurred.");
    }
}
