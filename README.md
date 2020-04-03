## SPITS 2.0

SPITS 2.0 is the next generation of the SPITS programming model, originally proposed by Borin et al. [1, 2].

The repository contains a) header files to support the development of SPITS applications (dev), b) examples of applications implemented using the SPITS programming model, c) a reference runtime implemented in python that is capable of executing SPITS applications in parallel using multiple computing nodes, d) tools to support the execution and performance analysis of SPITS applications.


BUILDING AND RUNNING THE EXAMPLES:

    The building scripts are simple and use g++ as the default compiler, you can change it if necessary.

FURTHER READING:

    Check the paper "Efficient and Fault Tolerant Computation of Partially Idempotent Tasks" to know more about the SPITS programming model and the problems it aims to solve.

## Publications

[1] BORIN, EDSON; RODRIGUES, IAN L. ; NOVO, ALBER T. ; SACRAMENTO, JOÃO D. ; BRETERNITZ, MAURICIO ; TYGEL, MARTIN . Efficient and Fault Tolerant Computation of Partially Idempotent Tasks. In: 14th International Congress of the Brazilian Geophysical Society & EXPOGEF, Rio de Janeiro, Brazil. 2015.

[2] BORIN, EDSON; BENEDICTO, C. ; RODRIGUES, IAN L. ; PISANI, F. ; TYGEL, MARTIN ; BRETERNITZ, MAURICIO . PY-PITS: A Scalable Python Runtime System for the Computation of Partially Idempotent Tasks. In: 5th Workshop on Parallel Programming Models, 2016, Los Angeles. Proceedings of the 5th Workshop on Parallel Programming Models, 2016.

[3] BENEDICTO, C. ; RODRIGUES, IAN L. ; TYGEL, MARTIN ; BRETERNITZ, MAURICIO ; BORIN, EDSON . Harvesting the computational power of heterogeneous clusters to accelerate seismic processing. In: 15th International Congress of the Brazilian Geophysical Society, 2017, Rio de Janeiro. Proceedings of the 15th International Congress of the Brazilian Geophysical Society, 2017.

[4] OKITA, N. T. ; RODAMILANS, C. B. ; COIMBRA, T. A. A. ; TYGEL, M. ; BORIN, E. . Otimização automática do custo de processamento de programas SPITS na AWS. In: XIX Simpósio em Sistemas Computacionais de Alto Desempenho, 2018, São Paulo. Anais do XIX Simpósio em Sistemas Computacionais de Alto Desempenho, 2018.

[5] OKITA, N. T. ; COIMBRA, T. A. A. ; RODAMILANS, C. B. ; TYGEL, M. ; BORIN, E. . Using SPITS to optimize the cost of high-performance geophysics processing on the cloud. In: EAGE Workshop on High Performance Computing for Upstream, 2018, Santander. Proceedings of the EAGE Workshop on High Performance Computing for Upstream, 2018.

[6] OKITA, NICHOLAS T. ; COIMBRA, TIAGO A. ; Rodamilans, Charles B. ; TYGEL, MARTIN ; BORIN, EDSON . Automatic Minimization of Execution Budgets of SPITS Programs in AWS. In: Calebe Bianchini; Carla Osthoff; Paulo Souza; Renato Ferreira. (Org.). Communications in Computer and Information Science. 1ed.: Springer International Publishing, 2020, v. 1171, p. 21-36.



** TODO: Fix this - it is pointing to the previous repository **
## Test results
Branch          | Build         | Tests coverage
----------------|-------------- | --------------
master          | [![Build Status](https://travis-ci.org/lmcad-unicamp/spits-2.0.svg?branch=master)](https://travis-ci.org/hpg-cepetro/spits-2.0) | [![codecov.io](https://codecov.io/github/lmcad-unicamp/spits-2.0/coverage.svg?branch=master)](https://codecov.io/github/lmcad-unicamp/spits-2.0)
develop         | [![Build Status](https://travis-ci.org/lmcad-unicamp/spits-2.0.svg?branch=develop)](https://travis-ci.org/lmcad-unicamp/spits-2.0) | [![codecov.io](https://codecov.io/github/lmcad-unicamp/spits-2.0/coverage.svg?branch=develop)](https://codecov.io/github/lmcad-unicamp/spits-2.0)
