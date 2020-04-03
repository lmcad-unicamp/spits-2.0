#!/bin/bash
mkdir -p run

mkdir -p run/spitz-pi-2
cd run/spitz-pi-2

echo Running second pi example...

CMD="../../spitz-python/spitz-run.sh ../../bin/spitz-pi-2/spitz-pi-2-module 100000"
echo $CMD
$CMD

