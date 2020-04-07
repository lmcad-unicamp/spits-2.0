#!/bin/bash
# The MIT License (MIT)
# 
# Copyright (c) 2020 Edson Borin <edson@ic.unicamp.br>
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
# 

mkdir -p bin

# Change this to your favorite compiler
COMPILER="g++"

ALLFLAGS="-g -O3 -I../../dev/include/"

SFLAGS="-DSPITS_SERIAL_DEBUG" # Flags for serial build

RFLAGS="-fPIC -shared"

mkdir -p bin

echo Building the getting-started application as a spits module...
$COMPILER $ALLFLAGS -o ./bin/getting-started-module ./main.cpp $RFLAGS || exit 1

echo Building the getting-started application as a serial binary...
$COMPILER $ALLFLAGS -o ./bin/getting-started-serial ./main.cpp $SFLAGS || exit 1

