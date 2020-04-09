#!/usr/bin/env python3

import sys
import os
import subprocess
import signal
import argparse
import socket

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
        os.unlink(os.path.join(SPITS_JOB_DIR, 'jm.exists'))
    except:
        pass

    os._exit(1)

def main(argv):
    global SPITS_JOB_DIR
    global spits_uid
    
    parser = argparse.ArgumentParser(description='Run SPITS job manager for jobs')
    
    parser.add_argument('--jobdir', '-j', action='store', 
        help='Default SPITS job directory to create jobs')
    parser.add_argument('--pypitsdir', '-p', action='store', 
        help='Default PYPITS root directory')
    parser.add_argument('--executable', '-e', action='store', default='python3',
        help='Default python executable')
    parser.add_argument('jobid', action='store', help='ID of the job to run')
    parser.add_argument('jmargs', nargs=argparse.REMAINDER, action='store', help='Additional jobmanager arguments')
    
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

    if not os.path.exists(os.path.join(expanded_dir, 'finished')):
        abort("Invalid job repository for job {} (without finished file)".format(args.jobid))

    if os.path.exists(os.path.join(expanded_dir, 'jm.exists')):
        abort("A job manager already exists for job {} (jm.exists file)".format(args.jobid))

    with open(os.path.join(expanded_dir, 'job'), 'r') as file:
        job_cmd = file.readline()

    addr = make_uid()
    spits_uid = "{}".format(addr)
    spits_output = "{}/logs/JM-{}.out".format(expanded_dir, spits_uid)
    spits_err = "{}/logs/JM-{}.err".format(expanded_dir, spits_uid)

    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    jm_dir = os.path.join(expanded_pypits_dir, 'jm.py')
    if not os.path.exists(jm_dir):
        abort("Job manager file does not exists at `{}`".format(jm_dir))
    
    run_args = [args.executable]
    run_args.append(jm_dir)
    run_args += args.jmargs
    run_args.append('--jobid={}'.format(args.jobid))
    run_args.append("--log={}".format(spits_err))
    run_args.append("--killtms=1")
    run_args.append("--announce=file")
    run_args += [x.strip() for x in job_cmd.split(' ')]

    with open(os.path.join(expanded_dir, 'finished'), 'w') as file:
        file.write('0')

    print(
"""
Using {} as UID
Using {} as default output
Using {} as default error output
Starting job manager for job {} with command: {}
""".format(spits_uid, spits_output, spits_err, args.jobid, run_args)
    )

    ret_val = 1
    try:
        os.chdir(expanded_dir)
        process = subprocess.Popen(run_args, stdout=open(spits_output, 'w'))
        PID = process.pid
        with open(os.path.join(expanded_dir, 'jm.exists'), 'w') as file:
            file.write(str(process.pid))
            
        process.wait()
        ret_val = process.returncode
    except BaseException as e:
        print("Caugth exception!")
        print(e)

    print("JobManager exited with status: {}".format(ret_val))

    try:
        os.unlink(os.path.join(expanded_dir, 'jm.exists'))
    except:
        pass

    try:
        os.unlink(os.path.join(expanded_dir, 'nodes', spits_uid))
    except:
        pass

    if ret_val == 0:
        with open(os.path.join(expanded_dir, 'finished'), 'w') as file:
            file.write('1')

        print("Job {} Finished!".format(args.jobid))

    return os._exit(ret_val)


if __name__ == '__main__':
    main(sys.argv)
