CXX=g++
# Common flags for all modes
ALLFLAGS=-g -O3 -I../../API/include/ -std=c++17 -fPIC
# Flags for serial build
SERIALFLAGS=-DSPITS_SERIAL_DEBUG
# Flags for module build
MODULEFLAGS=-fPIC -shared
# Program name
PROG=spits-pi
#INPUT FILE
CXXFILES=main.cpp

all: module serial
	@echo "Compilation successfull. Check bin/ directory"

module:
	mkdir -p bin/
	$(CXX) $(ALLFLAGS) -o bin/$(PROG)-module $(CXXFILES) $(MODULEFLAGS)

serial:
	mkdir -p bin/
	$(CXX) $(ALLFLAGS) -o bin/$(PROG)-serial $(CXXFILES) $(SERIALFLAGS)


clean:
	@rm -rf bin/*
