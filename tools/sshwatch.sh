#!/bin/bash

#
# MIT License
# 
# Copyright (c) 2017 Caian Benedicto
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

if [ "$#" -lt "4" ] ; then
    echo "USAGE: sshwatch.sh <cluster-address> <remote-spitz-dir> <local-spitz-dir> <local-starting-port> [delay]"
    exit 1
fi

id="sshw-$$"
cluster=$1
dir=$2/nodes/
tmp=/tmp/$id
tmp2=/tmp/$id-2
tmp3=/tmp/$id-3
out=$3/nodes/$id
port=$4
slp=${5:-60}

trap ctrl_c INT
function ctrl_c() {
    if [ "$sshpid" != "" ] ; then
        echo "Closing port forwarding..."
        kill $sshpid
    fi
    exit 0
}

while true ; do

    echo "Checking node list from $cluster:$dir"

    ssh $cluster "cat $dir/*" > $tmp 2>/dev/null

    if [ "$(cat $tmp 2>/dev/null)" == "" ] ; then
        echo "Remote nodes list is empty!"
    fi

    if [ "$(cat $tmp 2>/dev/null | wc -l)" != "$(cat $out 2>/dev/null | wc -l)" ] ; then

        rm -f $tmp2 $tmp3

        i=1
        while read p; do
            echo "-L $(($port+$i)):${p/node /}" >> $tmp2
            echo "node localhost:$(($port+$i))" >> $tmp3
            ((i+=1))
        done <$tmp

        echo "Remaking port forwarding..."

        if [ "$sshpid" != "" ] ; then
            kill $sshpid
        fi
        
        cmd="ssh $(cat $tmp2 | xargs) -N $cluster"
        echo "$cmd"
        $cmd &
        sshpid=$!

        cp $tmp3 $out
        echo "Node list updated."

    else
        echo "No changes found."
    fi
    sleep $slp
done

