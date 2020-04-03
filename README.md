# SPITS 2.0

The next generation of the SPITS programming model.

The repository contains examples to support the development and execution of modules using the SPITS programming model proposed by Borin et al. in "Efficient and Fault Tolerant Computation of Partially Idempotent Tasks".

SETTING UP FOR RUNNING:

    The building script uses g++ as the default compiler, you can change it if necessary.

    ** WARNING: We are changing the repository organization. This will be changed. **
    To build the examples, call build-examples.sh from inside the root of the bundle.
    A 'bin' directory will be created with all the examples.

RUNNING THE EXAMPLES:

    ** WARNING: We are changing the repository organization. This will be changed. **
    To run the examples, call run-X.sh from inside the root of the bundle. A 'run' directory will be created containing the result of the execution of each example.

    Additionally, the examples are compiled in to a self-contained, serial executable, they are named with a suffix '-serial' inside the 'bin' directory.

FURTHER READING:

    You may check the spits-runtime/pypits README file for more information on how to perform more elaborated executions and the README of each example.

Also check the paper "Efficient and Fault Tolerant Computation of Partially Idempotent Tasks" to know more about the SPITS programming model and the problems it aims to solve.


** TODO: Fix this - it is pointing to the previous repository **
## Test results
Branch          | Build         | Tests coverage
----------------|-------------- | --------------
master          | [![Build Status](https://travis-ci.org/hpg-cepetro/spitz.svg?branch=master)](https://travis-ci.org/hpg-cepetro/spitz) | [![codecov.io](https://codecov.io/github/hpg-cepetro/spitz/coverage.svg?branch=master)](https://codecov.io/github/hpg-cepetro/spitz)
develop         | [![Build Status](https://travis-ci.org/hpg-cepetro/spitz.svg?branch=develop)](https://travis-ci.org/hpg-cepetro/spitz) | [![codecov.io](https://codecov.io/github/hpg-cepetro/spitz/coverage.svg?branch=develop)](https://codecov.io/github/hpg-cepetro/spitz)
