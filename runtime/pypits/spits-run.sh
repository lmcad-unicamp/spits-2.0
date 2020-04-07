#!/bin/bash

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

###############################################################################
# Get the location of the spits scripts
###############################################################################
if [ -z "$SPITSDIR" ]
then
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
      DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
      SOURCE="$(readlink "$SOURCE")"
      [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    SPITSDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
fi

echo Using SPITSDIR=$SPITSDIR

###############################################################################
# Execution variables
###############################################################################

if [ "$#" -lt "2" ]
then
    echo "USAGE: run.sh module args"
    exit 1
fi

if [ -z "$SPITSARGS" ]
then
    SPITSARGS="--nw=0 --overfill=0 --announce=file --verbose=10 --jobid=123dasilva4"
fi

MODULE="$1"
MODULEARGS="${@:2}"

JMPATH="$SPITSDIR/jm.py"
TMPATH="$SPITSDIR/tm.py"
PYTHON="${PYTHON:-$(which python3)}"
PYTHON="${PYTHON:-$(which python)}"
PYTHONCMD="${PYTHONCMD:-$PYTHON -u}"

###############################################################################
# Generate a unique name to be used for the logs
###############################################################################

ADDR=$(hostname)
SPITSUID="$ADDR"-"$$"

echo Using $SPITSUID as UID

###############################################################################
# Set up nodes
###############################################################################
touch nodes.txt
mkdir -p nodes

###############################################################################
# Set up the logging
###############################################################################

mkdir -p log
JMSTZ="log/SPITS-JM-"$SPITSUID
JMOUT="log/STDOUT-JM-"$SPITSUID
JMERR="log/STDERR-JM-"$SPITSUID
TMSTZ="log/SPITS-TM-"$SPITSUID
TMOUT="log/STDOUT-TM-"$SPITSUID
TMERR="log/STDERR-TM-"$SPITSUID

###############################################################################
# Assemble the command line
###############################################################################
JMCMD="$PYTHONCMD $JMPATH --log=$JMSTZ $SPITSARGS $MODULE $MODULEARGS"
TMCMD="$PYTHONCMD $TMPATH --log=$TMSTZ --tmport=0 --announce=cat 
    $SPITSARGS $MODULE $MODULEARGS"

###############################################################################
# The following code automatically launches a Job Manager for the job and 
# prevents new Task Manager to run if the job is completed
# Be sure to remove 'jm.exists' and 'jm.finished' after each run
###############################################################################

JMEFILE="jm.exists"
JMFFILE="jm.finished"

# Kill subsequent task manager jobs
if [ -f $JMFFILE ]
then
    echo The Job inside this folder has already finished
    echo Please remove jm.exists and jm.finished
    exit 1
fi

echo Starting Job...

# Start a job manager if there is none 
if [ ! -f $JMEFILE ]
then
    echo $SPITSUID > $JMEFILE

    # Wait for an NFS sync
    sleep 10

    if [ "x$SPITSUID" = "x$(cat $JMEFILE)" ]
    then
        echo Launching Job Manager...
        echo Spits log set to $JMSTZ
        echo Stdout log set to $JMOUT
        echo Stderr log set to $JMERR
        echo $JMCMD
        $JMCMD 2>$JMERR | tee $JMOUT && touch $JMFFILE &
    fi
fi

# Start a task manager
echo Launching Task Manager...
echo Stdout log set to $TMOUT
echo Stderr log set to $TMERR
echo $TMCMD
$TMCMD 2>$TMERR | tee $TMOUT

# Wait for the job manager, if it was launched
wait

exit 0
