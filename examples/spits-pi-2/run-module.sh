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

MODULE=../bin/spits-pi-2-module

mkdir -p run || exit 1

cd run || exit 1

if [ ! -f ${MODULE} ]; then
    echo "Module file (${MODULE}) is missing. "
    echo "Try building it first by running ./build.sh"
    exit 1
fi

echo Running the spits-pi-2 module with PY-PITS...

CMD="../../../runtime/pypits/spits-run.sh ${MODULE} 100000"

echo $CMD

$CMD || exit 1


