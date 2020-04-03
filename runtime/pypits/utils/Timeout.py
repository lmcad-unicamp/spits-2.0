#!/usr/bin/env python

# The MIT License (MIT)
#
# Copyright (c) 2020 Edson Borin <edson@ic.unicamp.br>
# Copyright (c) 2015 Caian Benedicto <caian@ggaunicamp.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy 
# of this software and associated documentation files (the "Software"), to 
# deal in the Software without restriction, including without limitation the 
# rights to use, copy, modify, merge, publish, distribute, sublicense, 
# and/or sell copies of the Software, and to permit persons to whom the 
# Software is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in 
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
# IN THE SOFTWARE.


import logging
import traceback
import inspect
from threading import Timer
from threading import Lock

def timeout(timeout, callback, *args, **kwargs):
    if timeout is None or timeout <= 0:
        return TimeoutNoop()
    else:
        return Timeout(timeout, callback, *args, **kwargs)

class TimeoutNoop(object):
    def reset(self):
        pass
    def cancel(self):
        pass

class Timeout(object):
    def __init__(self, timeout, callback, args=[], kwargs={}):
        self.timeout = timeout
        self.callback = callback
        self._args = args
        self._kwargs = kwargs
        self._timer = None
        self._lock = Lock()

    def _callback(self):
        if self.callback(*self._args, **self._kwargs):
            self.reset()

    def reset(self):
        _frame = inspect.currentframe()
        frame = _frame.f_back
        fname = frame.f_code.co_filename
        fnum = frame.f_lineno
        self._lock.acquire()
        self._cancel()
        self._timer = Timer(self.timeout, self._callback)
        self._timer.start()
        self._lock.release()

    def _cancel(self):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def cancel(self):
        self._lock.acquire()
        self._cancel()
        self._lock.release()
