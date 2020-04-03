#!/bin/bash


echo "This script is deprecated. Each example should have its own build.sh and run-module.sh script."

exit 1

mkdir -p bin

COMPILER="g++" # Change this to your favorite compiler
ALLFLAGS="-g -O0 -I../dev/"
SFLAGS="-DSPITS_SERIAL_DEBUG" # Flags for serial build
RFLAGS="-fPIC -shared" # Flags for building a shared object

# Getting Started
#mkdir -p bin/getting-started
#echo Building getting-started as module...
#$COMPILER $ALLFLAGS -o ./bin/getting-started/getting-started-module ./examples/getting-started/main.cpp $RFLAGS || exit 1
#echo Building getting-started as serial...
#$COMPILER $ALLFLAGS -o ./bin/getting-started/getting-started-serial ./examples/getting-started/main.cpp $SFLAGS || exit 1

# spits-pi
mkdir -p bin/spits-pi
echo Building first pi example as module...
$COMPILER $ALLFLAGS -o ./bin/spits-pi/spits-pi-module ./examples/spits-pi/main.cpp $RFLAGS || exit 1
echo Building first pi example as serial...
$COMPILER $ALLFLAGS -o ./bin/spits-pi/spits-pi-serial ./examples/spits-pi/main.cpp $SFLAGS || exit 1

# spits-pi-2
#mkdir -p bin/spits-pi-2
#echo Building second pi example as module...
#$COMPILER $ALLFLAGS -o ./bin/spits-pi-2/spits-pi-2-module ./examples/spits-pi-2/main.cpp $RFLAGS || exit 1
#echo Building second pi example as serial...
#$COMPILER $ALLFLAGS -o ./bin/spits-pi-2/spits-pi-2-serial ./examples/spits-pi-2/main.cpp $SFLAGS || exit 1
