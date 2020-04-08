#!/usr/bin/env python3

import sys
import os
import socket
import jm
import contextlib
import io
import subprocess
import signal
import shutil
import argparse
import traceback

def make_uid():
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

def is_valid_spits_bin(spits_bin: str):
    if not os.path.exists(spits_bin) or not os.path.isfile(spits_bin):
        return False
    return True

def create_job_dir(job_dir: str, force: bool = False):
    print("Creating SPITS job `{}` at `{}`... ".format(job_dir.split('/')[-1], job_dir), end='')

    if os.path.exists(job_dir):
        if not force:
            print("Job `{}` already exists!".format(job_dir))
            return False
        try:
            print("Removing old existing directory... ", end='')
            if os.path.isfile(job_dir):
                os.unlink(job_dir)
            else:
                shutil.rmtree(job_dir)
        except Exception as e:
            print(traceback.format_exc(), e)
            return False

    try:
        os.makedirs(job_dir)
    except Exception as e:
        print(traceback.format_exc(), e)
        return False

    try:
        os.mkdir(os.path.join(job_dir, 'bin'))
        os.mkdir(os.path.join(job_dir, 'logs'))
        os.mkdir(os.path.join(job_dir, 'nodes'))
        with open(os.path.join(job_dir, 'finished'), 'w') as file:
            file.write("-1")
    except Exception as e:
        print(traceback.format_exc(), e)
        return False

    print("OK");
    return True

def copy_spits_files(job_dir: str, spits_bin: str, copy: bool):
    if not is_valid_spits_bin(spits_bin):
        print("Invalid SPITS binary: `{}`".format(spits_bin))
        return False

    try:
        if copy:
            shutil.copy(spits_bin, os.path.join(job_dir, 'bin'))
        else:
            spits_bin = os.path.abspath(os.path.expandvars(os.path.expanduser(spits_bin)))
            os.symlink(spits_bin, os.path.join(job_dir, 'bin', spits_bin.split('/')[-1]))
    except Exception as e:
        print(traceback.format_exc(), e)
        return False

    return True


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        return True

def main(argv):           
    parser = argparse.ArgumentParser(description='Create a SPITS job')
    
    parser.add_argument('--jobdir', '-j', action='store', 
        help='Default SPITS job directory to create jobs')
    parser.add_argument('--no-binary', '-n', action='store_true', default=False,
        help="Does not copy/link the binary. Create a job descriptor without it."
        "The binary must later be copied/linked to job's bin path with the same name as filename parameter")
    parser.add_argument('--copy', '-c', action='store_true', default=False,
        help="Copy binary to the job's bin directory instead of link it")
    parser.add_argument('jobid', action='store', 
        help='ID of the job to create')
    parser.add_argument('filename', action='store', 
        help="SPITS binary to run")
    parser.add_argument('args', nargs='*', action='store', 
        help='SPITS binary arguments')
    
    args = parser.parse_args()
        
    if args.jobdir is None:
        if 'SPITS_JOB_DIR' not in os.environ:
            abort("Spits job directory must be set by --jobdir parameter or by SPITS_JOB_DIR environment variable");
        else:
            args.jobdir = os.environ['SPITS_JOB_DIR']
            
    expanded_dir = os.path.expanduser(os.path.expandvars(os.path.join(args.jobdir, args.jobid)))

    if not create_job_dir(expanded_dir, force=True):
        abort("Error creating job birectory at `{}` for job {}".format(args.jobdir, args.jobid))

    if not args.no_binary and not copy_spits_files(expanded_dir, args.filename, args.copy):
        abort("Error copying SPITS binary to job directory `{}`".format(args.jobdir))
    try:
        with open(os.path.join(expanded_dir, 'job'), 'w') as f:
            f.write("{} {}".format(os.path.join('bin', args.filename.split('/')[-1]), ' '.join(args.args)))
    except Exception as e:
        print(traceback.format_exc(), e)
        abort("Crafting command for job {}".format(args.jobid))

    print("Job {} successfully created at `{}`".format(args.jobid, expanded_dir))
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
