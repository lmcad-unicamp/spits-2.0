#! /usr/bin/python3

import os
import sys
import json
import argparse
import glob
import traceback
import psutil

from libspits import SimpleEndpoint, messaging

connection_timeout = 10


def connect(host_addr: str, port: int) -> SimpleEndpoint:
    global connection_timeout
    try:
        endpoint = SimpleEndpoint(host_addr, port)
        endpoint.Open(connection_timeout)
        return endpoint

    except Exception as err:
        print(f"Impossible connect to `{host_addr}:{port}`: {err}")
        raise


def path_extend(*args) -> str:
    return os.path.expandvars(os.path.expanduser(os.path.join(*args)))


def abort(msg: str = ''):
    print(msg)
    os._exit(1)


def check_tm_process(pid_file: str, pid: str, cmd_line: str, node_file: str):
    pass


class ManagerProcess:
    def __init__(self, jobid: str, pidfile: str):
        self.jobid = jobid
        self.pidfile = pidfile
        with open(pidfile, 'r') as f:
            lines = f.readlines()
            pid = lines[0].strip()
            cmd_line = lines[1].strip()
            node_file = lines[2].strip()
        self.pid = int(pid)
        self.cmd_line = cmd_line
        self.node_file = node_file

    def check_process(self) -> bool:
        try:
            p = psutil.Process(self.pid)
            cmd = ' '.join(p.cmdline())
            if cmd != self.cmd_line:
                return False
            return p.is_running()
        except psutil.NoSuchProcess as e:
            return False

    def query_metrics(self) -> dict:
        raise NotImplementedError("Must be implemented in derived classes")

    def cleanup(self):
        raise NotImplementedError("Must be implemented in derived classes")


class TaskManagerProcess(ManagerProcess):
    def query_metrics(self) -> dict:
        with open(self.node_file, 'r') as f:
            value = f.readline()
            if value.startswith('node'):
                host_port = value.split(' ')[1]
                host, port = host_port.split(':')
                port = int(port)

        endpoint = connect(host, port)
        received_jobid = endpoint.ReadString(connection_timeout)
        endpoint.WriteString(self.jobid)
        if received_jobid != self.jobid:
            raise ValueError(
                f"Job id mismatch! Self: {self.jobid}, received: {received_jobid}")

        endpoint.WriteInt64(messaging.msg_cd_query_metrics_list)
        m_list = endpoint.ReadString(connection_timeout)
        m_list = json.loads(m_list)
        m_list['type'] = 'taskmanager'
        endpoint.Close()
        return m_list

    def cleanup(self):
        try:
            os.unlink(self.node_file)
        except:
            pass
        try:
            os.unlink(self.pidfile)
        except:
            pass


class JobManagerProcess(ManagerProcess):
    def query_metrics(self) -> dict:
        endpoint = connect('0.0.0.0', 7726)
        endpoint.WriteInt64(messaging.msg_cd_query_metrics_list)
        m_list = endpoint.ReadString(connection_timeout)
        m_list = json.loads(m_list)
        m_list['type'] = 'jobmanager'
        endpoint.Close()
        return m_list

    def cleanup(self):
        pass

class JobStatus:
    def __init__(self, jobid: str, job_file: str, finished_file: str):
        self.jobid = jobid
        self.job_file = job_file
        self.finished_file = finished_file

    def query_status(self) -> dict:
        with open(self.finished_file, 'r') as f:
            finished = False
            code = float(f.readline().strip())
            value = code
            if value == 0.0:
                value = "Maybe Running"
            elif value == -1.0:
                value = "Not running"
            else:
                value = f"Finished at {value}"
                finished = True

        with open(self.job_file, 'r') as f:
            job = '\n'.join([l.strip() for l in f.readlines()])

        return {
            'finished': finished,
            'code': code,
            'status': value,
            'job': job
        }



def main(argv):
    parser = argparse.ArgumentParser(
        description='List last metric values from all SPITS job/task managers'
                    'running a specified job in this system.')
    parser.add_argument('jobid', action='store', help='ID of the job.')
    parser.add_argument('--jobdir', action='store', default='~/spits-jobs/',
                        help='SPITS job directory (default: %(default)s)')

    args = parser.parse_args(argv)
    if 'SPITS_JOB_DIR' in os.environ:
        args.jobdir = os.environ['SPITS_JOB_DIR']

    spits_job_dir = path_extend(args.jobdir, args.jobid)
    pid_files = glob.glob(spits_job_dir + '/*.pid')
    status = path_extend(spits_job_dir, 'finished')

    managers = []
    metrics = {}
    for pidfile in pid_files:
        try:
            if os.path.basename(pidfile).startswith("TM-"):
                p = TaskManagerProcess(args.jobid, pidfile)
                managers.append(p)
            elif os.path.basename(pidfile).startswith("jm.pid"):
                p = JobManagerProcess(args.jobid, pidfile)
                managers.append(p)
        except Exception as e:
            error_val = {
                'ok': False,
                'error': str(e),
                'trace': str(traceback.format_exc())
            }
            metrics[os.path.basename(pidfile)] = error_val

    for p in managers:
        if p.check_process():
            m = p.query_metrics()
            x = {
                'ok': True,
                'metrics': m
            }
        else:
            p.cleanup()
            x = {
                'ok': False,
                'error': f"Process {p.pid} is invalid",
                'trace': ''
            }
        metrics[os.path.basename(p.pidfile)] = x

    job_file = path_extend(spits_job_dir, 'job')
    finished_file = path_extend(spits_job_dir, 'finished')
    j = JobStatus(args.jobid, job_file, finished_file)
    metrics['status'] = j.query_status()

    print(json.dumps(metrics, indent=4, sort_keys=True))

if __name__ == '__main__':
    main(sys.argv[1:])
    sys.exit(0)
