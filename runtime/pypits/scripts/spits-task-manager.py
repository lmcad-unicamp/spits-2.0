#!/usr/bin/env python3

import sys
import os
import subprocess
import signal
import argparse

SPITS_JOB_DIR = None
spits_uid = None
PID = None

def make_uid(pid=None):
    if not pid:
        pid = os.getpid()
    hostname = socket.gethostname()
    hostname = [c for c in hostname if c == ' ' or c == '-' or
        (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or
        (c >= '0' and c <= '9')]
    hostname = ''.join(hostname)
    return '%s-%s' % (hostname, pid)


def abort(msg: str = ''):
    print(msg)
    os._exit(1)

def exit_gracefully(signum, frame):
    global spits_uid
    global SPITS_JOB_DIR
    global PID
    
    print("Received signal {}. Exiting with code {}".format(signum, 1))   
    os.kill(PID, signal.SIGKILL)

    print("Cleaning files...")
    try:
        os.unlink(os.path.join(SPITS_JOB_DIR, 'nodes', make_uid(PID)))
    except:
        pass

    os._exit(1)

def main(argv):
    global SPITS_JOB_DIR
    global PID
    global spits_uid
    
    parser = argparse.ArgumentParser(description='Run SPITS task manager for jobs')
    
    parser.add_argument('--jobdir', '-j', action='store', 
        help='Default SPITS job directory to create jobs')
    parser.add_argument('--pypitsdir', '-p', action='store', 
        help='Default PYPITS root directory')
    parser.add_argument('--executable', '-e', action='store', default='python3',
        help='Default python executable')
    parser.add_argument('jobid', action='store', help='ID of the job to run')
    parser.add_argument('tmargs', nargs=argparse.REMAINDER, action='store', help='Additional taskmanager arguments')
    
    args = parser.parse_args()

    if args.jobdir is None:
        if 'SPITS_JOB_DIR' not in os.environ:
            abort("Spits job directory must be set by --jobdir parameter or by SPITS_JOB_DIR environment variable")
        else:
            args.jobdir = os.environ['SPITS_JOB_DIR']
    
    if args.pypitsdir is None:
        if 'PYPITS_DIR' not in os.environ:
            abort("Spits job directory must be set by --jobdir parameter or by PYPITS_DIR environment variable")
        else:
            args.pypitsdir = os.environ['PYPITS_DIR']
            
    expanded_dir = os.path.expanduser(os.path.expandvars(os.path.join(args.jobdir, args.jobid)))
    SPITS_JOB_DIR = expanded_dir
    expanded_pypits_dir =  os.path.expanduser(os.path.expandvars(args.pypitsdir))
    
    if not os.path.exists(os.path.join(expanded_dir, 'job')):
        abort("Invalid job repository for job {} (without job file)".format(args.jobid))
        
    with open(os.path.join(SPITS_JOB_DIR, 'job'), 'r') as file:
        job_cmd = file.readline()

    addr = make_uid()
    spits_uid = "{}".format(addr)
    spits_output = "{}/logs/TM-{}.out".format(expanded_dir, spits_uid)
    spits_err = "{}/logs/TM-{}.err".format(expanded_dir, spits_uid)

    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    tm_dir = os.path.join(expanded_pypits_dir, 'tm.py')
    if not os.path.exists(tm_dir):
        abort("Task manager file does not exists at `{}`".format(tm_dir))
    
    run_args = [args.executable]
    run_args.append(tm_dir)
    run_args += args.tmargs
    run_args.append('--jobid={}'.format(args.jobid))
    run_args.append("--log={}".format(spits_err))
    run_args.append("--announce=file")
    run_args += [x.strip() for x in job_cmd.split(' ')]

    print(
"""
Using {} as UID
Using {} as default output
Using {} as default error output
Starting task manager for job {} with command: {}
""".format(spits_uid, spits_output, spits_err, args.jobid, run_args)
        )

    ret_val = 1
    try:
        os.chdir(expanded_dir)
        process = subprocess.Popen(run_args, stdout=open(spits_output, 'w'))
        PID = process.pid
        process.wait()
        ret_val = process.returncode
    except BaseException as e:
        print("Caugth exception!")
        print(e)

    print("TaskManager exited with status: {}".format(ret_val))

    try:
        os.unlink(os.path.join(SPITS_JOB_DIR, 'nodes', make_uid(PID)))
    except:
        pass

    return os._exit(ret_val)

if __name__ == '__main__':
    main(sys.argv)
