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

from utils import log_lines

import logging
import sys
import traceback

_memstat_enabled = False
_can_enable = True
_sc_page_size = 0

try:
    import resource
    import gc
    import base64
    import os
    import inspect
    _sc_page_size = os.sysconf('SC_PAGE_SIZE')
except:
    logging.warning('could not import the modules necessary to memstat, disabling')
    _memstat_enabled = False
    _can_enable = False


def disable():
    global _memstat_enabled
    _memstat_enabled = False

def enable():
    global _memstat_enabled, _can_enable
    if not _can_enable:
        logging.warning('could not enable memstat')
    else:
        _memstat_enabled = True

def isenabled():
    global _memstat_enabled
    return _memstat_enabled

def my_getrss():
    """ See http://stackoverflow.com/questions/669438/how-to-get-memory-usage-at-run-time-in-c """
    fp = None
    try:
        fp = open("/proc/self/stat")
        parts = fp.read().split()
        return int(parts[23]) * _sc_page_size / 1024
    finally:
        if fp:
            fp.close()

def stats():
    global _memstat_enabled, _can_enable
    if not _memstat_enabled:
        return
    if not _can_enable:
        logging.warning('could not enable memstat')
        _memstat_enabled = False
        return
    try:
        s0, s1, s2 = gc.get_count()
        usage = resource.getrusage(resource.RUSAGE_SELF)
        kb = usage.ru_maxrss
        if kb == 0:
            kb = my_getrss()
        _frame = inspect.currentframe()
        frame = _frame.f_back
        fname = frame.f_code.co_filename
        fnum = frame.f_lineno
        logging.info('memstat:%s:%d: rss_kb: %d gb_stages: %d %d %d' % \
                     (fname, fnum, kb, s0, s1, s2))
    except:
        log_lines(sys.exc_info(), logging.debug)
        log_lines(traceback.format_exc(), logging.debug)
        logging.warning('something went wrong with memstat, disabling')
        _can_enable = False
        _memstat_enabled = False
    finally:
        # Necessary to avoid cyclic references and leak memory!
        del frame
        del _frame
