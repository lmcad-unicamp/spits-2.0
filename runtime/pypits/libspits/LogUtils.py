#!/usr/bin/env python

# The MIT License (MIT)
#
# Copyright (c) 2020 Edson Borin <edson@ic.unicamp.br>
# Copyright (c) 2018 Caian Benedicto <caian@ggaunicamp.com>
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
import coloredlogs


def get_logger(name):
    return logging.getLogger(name)


def setup_log(verbosity_level: int = 0, filename: str = None):
    # 0 -> WARNING, 1->INFO, 2->DEBUG
    formatter = '[%(asctime)s] [%(name)s] [%(levelname)s] [%(threadName)s]: ' \
                '%(message)s'
    if verbosity_level <= 0:
        log_level = logging.ERROR
    elif verbosity_level == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    if filename:
        logging.basicConfig(
            filename=filename, filemode='w', level=log_level, format=formatter)
    else:
        logging.basicConfig(level=log_level, format=formatter)
        coloredlogs.install(level=log_level, format=formatter)


def log_lines(text, dest):
    for line in text.split('\n'):
        dest(line.strip())

