# SPITS 2.0 Tools

This is a set of tools to support the execution and performance analysis of SPITS applications.

## `perf-pits.py`: A PY-PITS performance diagnosys tool

This tool summarizes performance statistics from logs produced by PY-PITS
during the execution of a SPITS job.

### USAGE

Run `perf-pits.py -h` to read the usage.

```
python3 perf-pits.py -h
```

### EXAMPLE

In this example we will execute the spits-pi application (located at the "examples" folder) and analyse its logs using the perf-pits.py script.

First, we change into the spits-pi directory. Assuming we are at the tools directory:

```
cd ../examples/spits-pi
```

Then, we build and run the spits-pi application:

```
./build.sh
./run-module.sh
```

Then, we execute the perf-spits script

```
python3 ../../tools/perf-pits.py run/log/SPITS*
```

The results will be printed on the standard output

```
Metric, %, total, average, std, min, max
Init overhead, 1.99, 0.772000, 0.772000, 0.000000, 0.772000, 0.772000
Useful work, 51.05, 19.767000, 19.767000, 0.000000, 19.767000, 19.767000
Duplicated work, 4.65, 1.799000, 1.799000, 0.000000, 1.799000, 1.799000
Starving, 42.31, 16.382000, 16.382000, 0.000000, 16.382000, 16.382000
```

## `sshwatch.sh`

A SSH based command-line tool to monitor the execution remote PY-PITS jobs

