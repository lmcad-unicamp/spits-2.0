#!/usr/bin/env python3

import sys
import os
import shutil
import argparse
import traceback


def abort(msg: str = ''):
    print(msg)
    sys.exit(1)


def is_valid_spits_bin(spits_bin: str) -> bool:
    if not os.path.exists(spits_bin) or not os.path.isfile(spits_bin):
        return False
    return True


def create_job_dir(jobs_dir: str, job_id: str, force: bool = False) -> str:
    print(f"Creating job {job_id} at {jobs_dir}")
    job_dir = path_extend(jobs_dir, job_id)
    if os.path.exists(job_dir):
        if not force:
            raise Exception(f"Job {job_id} already exists in {jobs_dir}")
        else:
            shutil.rmtree(job_dir, ignore_errors=True)

    dirs_to_create = [
        path_extend(job_dir, 'bin'),
        path_extend(job_dir, 'logs'),
        path_extend(job_dir, 'nodes')
    ]
    for d in dirs_to_create:
        print(f"Creating directory: {d}...")
        os.makedirs(d, exist_ok=True)
    with open(path_extend(job_dir, 'finished'), 'w') as f:
        f.write('-1')

    print("Directories created!")
    return job_dir


def copy_spits_files(job_dir: str, spits_bin: str, copy: bool):
    if not is_valid_spits_bin(spits_bin):
        raise FileNotFoundError(f"Invalid SPITS binary at: `{spits_bin}`")

    if copy:
        dest = os.path.join(job_dir, 'bin')
        print(f"Copying files from `{spits_bin}` to `{dest}`")
        shutil.copy(spits_bin, dest)
    else:
        spits_bin = os.path.abspath(path_extend(spits_bin))
        link_path = os.path.join(job_dir, 'bin', os.path.basename(spits_bin))
        print(f"Linking `{spits_bin}` to `{link_path}`")
        os.symlink(spits_bin, link_path)


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        return True


def path_extend(*args) -> str:
    return os.path.expandvars(os.path.expanduser(os.path.join(*args)))


def main(argv):
    parser = argparse.ArgumentParser(description='Create a SPITS job')

    parser.add_argument('jobid', action='store', help='ID of the job to create')
    parser.add_argument('--jobdir', action='store', default='~/spits-jobs/',
                        help='SPITS job directory (default: %(default)s)')
    parser.add_argument('--no-binary', '-n', action='store_true', default=False,
                        help="Does not copy/link the binary. Create a job descriptor "
                             "without it. The binary must later be copied/linked "
                             "to job's bin path with the same name as filename "
                             "parameter")
    parser.add_argument('--copy', '-c', action='store_true', default=False,
                        help="Copy binary to the job's bin directory instead of "
                             "link it")
    parser.add_argument('--force', '-f', action='store_true', default=False,
                        help="Force job creation even it path already exists")
    parser.add_argument('filename', action='store', help="SPITS binary to run")
    parser.add_argument('args', nargs='*', action='store',
                        help='SPITS binary arguments')

    args = parser.parse_args(argv)
    if 'SPITS_JOB_DIR' in os.environ:
        args.jobdir = os.environ['SPITS_JOB_DIR']

    job_dir = create_job_dir(args.jobdir, args.jobid, args.force)
    binary_file = os.path.join('bin', os.path.basename(args.filename))

    if not args.no_binary:
        copy_spits_files(job_dir, args.filename, args.copy)
    else:
        print(f"No copy will be performed. Do not forget to place your SPITS "
              f"binary at `{os.path.join(job_dir, binary_file)}` before "
              f"executing SPITS!")

    with open(os.path.join(job_dir, 'job'), 'w') as f:
        f.write(f"{binary_file} {' '.join(args.args)}")

    print(f"Job {args.jobid} successfully created at `{job_dir}`")
    return 0


if __name__ == '__main__':
    ret = main(sys.argv[1:])
    sys.exit(ret)
