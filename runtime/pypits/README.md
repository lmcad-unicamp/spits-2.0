# PY-PITS

PY-PITS is a simple runtime and SPITS application manager used to run SPITS applications.

## Requirements

PY-PITS uses Python 3.7+ and the requeriments are listed in `requirements.txt` file.

## Running PYPITS

### Using Job manager and task manager directly
To run an SPITS application using PY-PITS, you may use `jm.py` and `tm.py` scripts directly to create a job manager and a task manager, respectively. You may refer to these scripts about the usage  

### Using SPITS Application Manager to start job manager and task managers easilly
To run a SPITS application, you may need:

1. **Create a Job**: A job have an unique name, a SPITS binary and SPITS binary's arguments. Use the `spits-create-job.py` to create and SPITS' job and all directory structure used to run job manager and task manager. All jobs will be created at `~/spits-jobs` and the directory can be changed with the `--jobdir` parameter.

Example that will create a job named jobpi and execute the spits binary at `<..>/spits-pi-module` with the folowing arguments passed to the binary `10000 10`: 
::
	
	`./spits-job-create.py jobpi ~/spits-2.0/examples/spits-pi/bin/spits-pi-module "10000 10"`

2. **Start a Job Manager**: Once a job is created, you can start a job manager refering to it using the job identificator. You may use the `spits-job-manager.py` script to execute a SPITS job manager. Besides the jobid argument the others arguments are passed to the job manager.

Example to start a job manager from a job called jobpi, passing the argument "--verbose=3" to the job manager. See `jm.py` script to check all available parameters that can be passed to job manager.
::

	`./spits-job-manager.py jobpi --verbose=3`

3. **Start a Task Manager**: Once a job is created, you can start a task manager refering to it using the job identificator. You may use the `spits-task-manager.py` script to execute a SPITS task manager. Besides the jobid argument the others arguments are passed to the task manager.

Example to start a task manager from a job called jobpi, passing the argument "--verbose=3" to the task manager. See `tm.py` script to check all available parameters that can be passed to task manager.
::

        `./spits-task-manager.py jobpi --verbose=3`

---
**NOTE**

* All outputs will be placed at `~/spits-jobs/jobpi/logs/`. The `.err` and `.out` files stores the error and standard outputs of the job manager (and consequently from the spits application). The `.log` stores the PY-PITS job manager logs, using python logging module and the `.metrics.json` stores the last recorded metrics from job manager before the exit.

* The SPITS job will run relative to `~/spits-jobs/jobpi/`

---

4. **Check Job Status and metrics**:  Use the `spits-job-status.py` script.

