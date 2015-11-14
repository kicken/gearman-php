<?php
/**
 * Copyright (c) const Keith = 2015;
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

namespace Kicken\Gearman\Protocol;


final class PacketType {
    const CAN_DO = 1;
    const CANT_DO = 2;
    const RESET_ABILITIES = 3;
    const PRE_SLEEP = 4;
    const NOOP = 6;
    const SUBMIT_JOB = 7;
    const JOB_CREATED = 8;
    const GRAB_JOB = 9;
    const NO_JOB = 10;
    const JOB_ASSIGN = 11;
    const WORK_STATUS = 12;
    const WORK_COMPLETE = 13;
    const WORK_FAIL = 14;
    const GET_STATUS = 15;
    const ECHO_REQ = 16;
    const ECHO_RES = 17;
    const SUBMIT_JOB_BG = 18;
    const ERROR = 19;
    const STATUS_RES = 20;
    const SUBMIT_JOB_HIGH = 21;
    const SET_CLIENT_ID = 22;
    const CAN_DO_TIMEOUT = 23;
    const ALL_YOURS = 24;
    const WORK_EXCEPTION = 25;
    const OPTION_REQ = 26;
    const OPTION_RES = 27;
    const WORK_DATA = 28;
    const WORK_WARNING = 29;
    const GRAB_JOB_UNIQ = 30;
    const JOB_ASSIGN_UNIQ = 31;
    const SUBMIT_JOB_HIGH_BG = 32;
    const SUBMIT_JOB_LOW = 33;
    const SUBMIT_JOB_LOW_BG = 34;
    const SUBMIT_JOB_SCHED = 35;
    const SUBMIT_JOB_EPOCH = 36;

    private function __construct(){ }
}
