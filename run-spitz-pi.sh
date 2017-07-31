#!/bin/bash
mkdir -p run

mkdir -p run/spitz-pi
cd run/spitz-pi

echo Running first pi example...

CMD="../../spitz-python/spitz-run.sh ../../bin/spitz-pi/spitz-pi-module 100000"
echo $CMD
$CMD


