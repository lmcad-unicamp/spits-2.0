#!/usr/bin/env python3

import sys
import os
import argparse
import json

def abort(msg: str = ''):
    print(msg)
    os._exit(1)

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
    parser = argparse.ArgumentParser(description='Get Status of SPITS job')
    
    parser.add_argument('--jobdir', '-j', action='store', 
        help='Default SPITS job directory to create jobs')
    parser.add_argument('jobid', action='store', help='ID of the job to query status')
    
    args = parser.parse_args()
        
    if args.jobdir is None:
        if 'SPITS_JOB_DIR' not in os.environ:
            abort("Spits job directory must be set by --jobdir parameter or by SPITS_JOB_DIR environment variable");
        else:
            args.jobdir = os.environ['SPITS_JOB_DIR']
            
    expanded_dir = os.path.expanduser(os.path.expandvars(os.path.join(args.jobdir, args.jobid)))
    values = dict()
    
    jm_pid = 0
    job_cmd = ''
    status = 0
    tms = []
    
    values['path'] = expanded_dir
    
    if not os.path.exists(expanded_dir):
        print("Invalid directory `{}`".format(expanded_dir))
        return 1

    try:
        with open(os.path.join(expanded_dir, 'jm.exists'), 'r') as f:
            jm_pid = int(f.readline().strip())
    except Exception as e:
        pass
    values['job_manager_pid'] = jm_pid
    
    try:
        with open(os.path.join(expanded_dir, 'job'), 'r') as f:
            job_cmd = f.readline().strip()
    except Exception as e:
        pass
    values['command'] = job_cmd
    
    try:
        with open(os.path.join(expanded_dir, 'finished'), 'r') as f:
            status = int(f.readline().strip())
    except Exception as e:
        pass
    values['finished'] = False if status != 1 else True
    
    try:
        for root, dirs, files in os.walk(os.path.join(expanded_dir, 'nodes')):
            for f in files:
                expanded_file = os.path.join(root, f)
                for line in open(expanded_file, 'r').readlines():
                    stripped = line.strip()
                    if stripped.startswith('node'):
                        tms.append([f, stripped.split(' ')[1]])
    except Exception as e:
        print(e)
    values['nodes'] = tms
    
    print(json.dumps(values))
    
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
