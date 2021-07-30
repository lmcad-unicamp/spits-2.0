#!/usr/bin/env python3

import sys
import os
import subprocess
import signal
import argparse
import socket
import time

SPITS_JOB_DIR = None
SPITS_UID = None
PID = None
PIDFILE = None
RETCODE_FILE = None


def make_uid(pid=None):
    if not pid:
        pid = os.getpid()
    hostname = socket.gethostname()
    hostname = [c for c in hostname if c == ' ' or c == '-' or
                ('a' <= c <= 'z') or ('A' <= c <= 'Z') or
                ('0' <= c <= '9')]
    hostname = ''.join(hostname)
    return f'{hostname}-{pid}'


def path_extend(*args) -> str:
    return os.path.expandvars(os.path.expanduser(os.path.join(*args)))


def abort(msg: str = ''):
    print(msg)
    os._exit(1)


def remove_files():
    global PID
    global PIDFILE

    if PID is not None:
        try:
            print(f"Killing {PID}...")
            os.kill(PID, signal.SIGKILL)
        except:
            pass
    if PIDFILE is not None:
        try:
            print(f"Removing PID file at `{PIDFILE}`")
            os.unlink(PIDFILE)
        except:
            pass


def write_retcode(code: int):
    global RETCODE_FILE
    if RETCODE_FILE is not None:
        with open(RETCODE_FILE, 'w') as f:
            f.write(str(code))


def exit_gracefully(signum, frame):
    print(f"Received signal {signum}. Exiting with code: 1")
    remove_files()
    write_retcode(1)
    os._exit(1)


def main(argv):
    global SPITS_JOB_DIR
    global SPITS_UID
    global PID
    global PIDFILE
    global RETCODE_FILE

    jm_executable = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'jm.py'
    )
    parser = argparse.ArgumentParser(description='Run SPITS job manager for jobs')
    parser.add_argument('jobid', action='store', help='ID of the job to run')
    parser.add_argument('--jobdir', action='store',
                        default=path_extend('~/spits-jobs/'),
                        help='SPITS job directory (default: %(default)s)')
    parser.add_argument('--jm-script', action='store', default=jm_executable,
                        help='JobManager directory (default: %(default)s)')
    parser.add_argument('--python', action='store', default=sys.executable,
                        help='Python executable (default: %(default)s)')
    parser.add_argument('--force', '-f', action='store_true', default=False,
                        help='Force starting job manager even if jm.pid file '
                             'exists (default: %(default)s)')
    parser.add_argument('jmargs', nargs=argparse.REMAINDER, action='store',
                        help='Additional Job Manager arguments')

    args = parser.parse_args(argv)
    if 'SPITS_JOB_DIR' in os.environ:
        args.jobdir = os.environ['SPITS_JOB_DIR']
    if 'PYPITS_DIR' in os.environ:
        args.pypitsdir = os.environ['PYPITS_DIR']

    SPITS_JOB_DIR = path_extend(args.jobdir, args.jobid)
    SPITS_UID = f"JM-{make_uid()}"
    jm_executable = args.jm_script
    logs_dir = path_extend(SPITS_JOB_DIR, 'logs')
    job_file = path_extend(SPITS_JOB_DIR, 'job')
    finished_file = path_extend(SPITS_JOB_DIR, 'finished')
    spits_output = path_extend(logs_dir, f"{SPITS_UID}.out")
    spits_err = path_extend(logs_dir, f"{SPITS_UID}.err")
    spits_log = path_extend(logs_dir, f"{SPITS_UID}.log")
    metrics_file = path_extend(logs_dir, f"{SPITS_UID}.metrics.json")
    PIDFILE = path_extend(SPITS_JOB_DIR, f"jm.pid")
    RETCODE_FILE = path_extend(logs_dir, f"{SPITS_UID}.ret")

    if not os.path.exists(job_file):
        abort(f"Invalid job repository for job {args.jobid} (no job file)")
    if not os.path.exists(jm_executable):
        abort(f"Job manager file does not exists at `{jm_executable}`")
    if os.path.exists(PIDFILE) and not args.force:
        abort("Job manager already exists. There is a job manager PID file at "
              f"{PIDFILE}")
    with open(os.path.join(SPITS_JOB_DIR, 'job'), 'r') as file:
        job_cmd = file.readline()

    run_args = [args.python, jm_executable] + args.jmargs
    run_args.append("--killtms")
    run_args += ['--jobid', args.jobid]
    run_args += ['--log', spits_log]
    run_args += ['--metrics-file', metrics_file]
    run_args += [x.strip() for x in job_cmd.split(' ')]
    cmd_line = ' '.join(run_args)

    with open(finished_file, 'w') as file:
        file.write('0')

    print(
        f"""Using {SPITS_UID} as Task Manager UID
Python: {args.python}
Job Manager executable: {jm_executable}
PID File: {PIDFILE}
Using {spits_output} as default output
Using {spits_err} as default error output
Using {spits_log} as default task manager log outputs
Metrics will be dumped to: {metrics_file}""")

    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    ret_val = 1
    try:
        print(f"Changing CWD to {SPITS_JOB_DIR}...")
        os.chdir(SPITS_JOB_DIR)
        print(f"Running command: {cmd_line}")
        stdout = open(spits_output, 'w')
        stderr = open(spits_err, 'w')
        process = subprocess.Popen(run_args, stdout=stdout, stderr=stderr)
        PID = process.pid
        with open(PIDFILE, 'w') as f:
            f.write(f"{PID}\n{cmd_line}\n\n")
        process.wait()
        ret_val = process.returncode
        if ret_val == 0:
            with open(finished_file, 'w') as f:
                f.write(str(time.time()))
    except BaseException as e:
        print(f"Caught exception: {e}")
    finally:
        remove_files()
        write_retcode(ret_val)

    print(f"Job Manager exited with status: {ret_val}")
    os._exit(ret_val)


if __name__ == '__main__':
    main(sys.argv[1:])
