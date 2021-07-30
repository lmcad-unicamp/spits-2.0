#!/usr/bin/python
# !/usr/bin/env python

# The MIT License (MIT)
#
# Copyright (c) 2020 Otávio Napoli <otavio.napoli@gmail.com>
# Copyright (c) 2020 Edson Borin <edson@ic.unicamp.br>
# Copyright (c) 2015 Caian Benedicto <caian@ggaunicamp.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.
import argparse
import uuid
import time
from datetime import datetime

from libspits import JobBinary, setup_log, get_logger, Pointer
from libspits import Listener, TaskPool
from libspits import messaging, config
from libspits import timeout as Timeout
from libspits import make_uid
from libspits import log_lines
from libspits import PerfModule

import sys, os, socket, logging, multiprocessing, traceback, json

from threading import Lock

from libspits.JobBinary import MetricManager

try:
    import Queue as queue  # Python 2
except ImportError:
    import queue  # Python 3

# Global configuration parameters
tm_mode = ''  # Addressing mode
tm_addr = ''  # Bind address
tm_port = 0  # Bind port
tm_nw = 0       # Maximum number of workers
tm_overfill = 0  # Extra space in the task queue
tm_announce = None  # Mechanism used to broadcast TM address
tm_log_file = None  # Output file for logging
tm_verbosity = 0    # Verbosity level for logging
tm_conn_timeout = None  # Socket connect timeout
tm_recv_timeout = None  # Socket receive timeout
tm_send_timeout = None  # Socket send timeout
tm_timeout = None
tm_profiling = None  # 1 to enable profiling
tm_perf_rinterv = None  # Profiling report interval (seconds)
tm_perf_subsamp = None  # Number of samples collected between report intervals
tm_jobid = None
tm_name = None
tm_spits_profile_buffer_size = None
tm_announce_filename = ''

spits_binary = ''
spits_binary_args = []

metrics_file = None
tm_hostname = None
METRICS = None

logger = get_logger(os.path.basename(sys.argv[0]))

###############################################################################
# Parse global configuration
###############################################################################
def parse_global_config(arguments):
    global tm_mode, tm_addr, tm_port, tm_nw, tm_log_file, tm_verbosity, \
        tm_overfill, tm_announce, tm_conn_timeout, tm_recv_timeout, \
        tm_send_timeout, tm_timeout, tm_profiling, tm_perf_rinterv, \
        tm_perf_subsamp, tm_jobid, tm_spits_profile_buffer_size, tm_name, \
        spits_binary, spits_binary_args, tm_announce_filename, metrics_file, \
        tm_hostname

    parser = argparse.ArgumentParser(description="SPITS Task Manager runtime")
    parser.add_argument('binary', metavar='PATH', type=str,
                        help='Path to the SPITS binary')
    parser.add_argument('binary_args', metavar='ARGS', type=str,
                        nargs=argparse.REMAINDER,
                        help='SPITS binary arguments')
    parser.add_argument('--tmmode', action='store', metavar='MODE',
                        type=str, default=config.mode_tcp,
                        help="Task manager addressing mode "
                             "(default: %(default)s)")
    parser.add_argument('--tmaddr', action='store', metavar='ADDRESS',
                        type=str, default='0.0.0.0',
                        help="Task manager bind address (default: %(default)s)")
    parser.add_argument('--tmport', action='store', metavar='PORT',
                        type=int, default=0,
                        help="Task manager bind port (default: %(default)s)")
    parser.add_argument('--nw', action='store', metavar='WORKERS',
                        type=int, default=multiprocessing.cpu_count(),
                        help="Maximum number of workers (default: %(default)s)")
    parser.add_argument('--tm-overfill', action='store', metavar='EXTRA',
                        type=int, default=0,
                        help="Extra space in the task queue (default: %(default)s)")
    parser.add_argument('--announce', action='store', metavar='TYPE',
                        type=str, default=config.announce_file,
                        help="Mechanism used to broadcast TM address "
                             "(default: %(default)s)")
    parser.add_argument('--log', action='store', type=str, metavar='PATH',
                        help="Redirect log messages to a file")
    parser.add_argument('--verbose', action='store', metavar='LEVEL',
                        type=int, default=config.def_verbosity,
                        help="Verbosity level (default: %(default)s)")
    parser.add_argument('--ctimeout', action='store', metavar='TIME',
                        type=int, default=config.def_connection_timeout,
                        help="Socket connect timeout (default: %(default)s)")
    parser.add_argument('--rtimeout', action='store', metavar='TIME',
                        type=int, default=config.def_receive_timeout,
                        help="Socket receive timeout (default: %(default)s)")
    parser.add_argument('--stimeout', action='store', metavar='TIME',
                        type=int, default=config.def_send_timeout,
                        help="Socket send timeout (default: %(default)s)")
    parser.add_argument('--idle-timeout', action='store', metavar='TIME',
                        type=int, default=config.def_idle_timeout,
                        help="Terminate task manager when idle for timeout "
                             "seconds (default: %(default)s)")
    parser.add_argument('--profile', action='store_true', default=False,
                        help="Enable job manager profiling (default: %(default)s)")
    parser.add_argument('--perf-interval', action='store', metavar='TIME',
                        type=int, default=60,
                        help="Profiling report interval (in seconds) "
                             "(default: %(default)s)")
    parser.add_argument('--perf-subsamp', action='store', metavar='TIME',
                        type=int, default=12,
                        help="Number of samples collected between report "
                             "intervals (default: %(default)s)")
    parser.add_argument('--name', action='store', type=str, default='',
                        help="Name for the Job Manager")
    parser.add_argument('--jobid', action='store', metavar='ID',
                        type=str, default='', help="ID of the job")
    parser.add_argument('--metric-buffer', action='store', type=int, default=10,
                        help="Number of elements for each metric buffer "
                             "(default: %(default)s)")
    parser.add_argument('--announce-file', action='store', type=str,
                        help='Name of the file to announce')
    parser.add_argument('--metrics-file', action='store', type=str,
                        help="Dump metrics to file when process ends")
    parser.add_argument('--hostname', action='store', type=str,
                        help="Hostname to use")

    args = parser.parse_args(arguments)
    spits_binary = args.binary
    if not os.path.isfile(spits_binary):
        raise ValueError(f"Invalid spits binary at: {spits_binary}")
    spits_binary_args = args.binary_args
    tm_mode = args.tmmode
    tm_addr = args.tmaddr
    tm_port = args.tmport
    tm_nw = args.nw
    tm_overfill = args.tm_overfill
    tm_announce = args.announce
    tm_log_file = args.log
    tm_verbosity = args.verbose
    tm_conn_timeout = args.ctimeout
    tm_recv_timeout = args.rtimeout
    tm_send_timeout = args.stimeout
    tm_timeout = args.idle_timeout
    tm_profiling = args.profile
    tm_perf_rinterv = args.perf_interval
    tm_perf_subsamp = args.perf_subsamp
    tm_name = args.name or f'Task-Manager-{str(uuid.uuid4()).replace("-", "")}'
    tm_jobid = args.jobid
    tm_spits_profile_buffer_size = args.metric_buffer
    tm_announce_filename = args.announce_file
    tm_hostname = args.hostname
    metrics_file = args.metrics_file


###############################################################################
# Abort the aplication with message
###############################################################################
def abort(error):
    logger.critical(error)
    exit(1)


###############################################################################
# Append the node address to the nodes list
###############################################################################
def announce_cat(addr: str, filename: str = None):
    # Override the filename if it is empty
    if filename is None:
        nodefile = 'nodes.txt'
        filename = os.path.join('.', nodefile)
        logger.debug(f'Appending node {addr} to file {nodefile}...')

    try:
        f = open(filename, "a")
        f.write("node %s\n" % addr)
        f.close()
    except:
        logger.warning(f'Failed to write to {filename}!')


###############################################################################
# Add the node to a directory as a file
###############################################################################
def announce_file(addr: str):
    global tm_announce_filename
    # Override the dirname if it is empty
    if tm_announce_filename is None:
        dirname = 'nodes'
        os.makedirs(dirname, exist_ok=True)
        # Create a unique filename for the process
        tm_announce_filename = os.path.join('.', dirname, make_uid())

    logger.info(f"Adding node {addr} to directory {tm_announce_filename}...")
    try:
        with open(tm_announce_filename, 'w') as f:
            f.write(f"node {addr}\n")
    except:
        logger.warning(f'Failed to write to {tm_announce_filename}!')


def terminate():
    global metrics_file, METRICS

    if metrics_file is not None:
        with open(metrics_file, 'w') as f:
            metrics_final = METRICS.get_metrics()
            json.dump(metrics_final, f, indent=4, sort_keys=True)

    os._exit(0)


###############################################################################
# Server callback
###############################################################################
def server_callback(conn, addr, port, job, metrics, tpool, cqueue, timeout):
    global tm_recv_timeout, tm_send_timeout
    logger.debug('Connected to {}:{}.'.format(addr, port))

    try:
        # Send the job identifier
        conn.WriteString(tm_jobid)

        # Verify job id of the answer
        jobid = conn.ReadString(tm_recv_timeout)

        if tm_jobid != jobid:
            logger.error(f'Job Id mismatch from {conn.address}:{conn.port}! '
                         f'Self: {tm_jobid}, other: {jobid}!')
            conn.Close()
            return False

        # Read the type of message
        mtype = conn.ReadInt64(tm_recv_timeout)
        timeout.reset()

        # Termination signal
        if mtype == messaging.msg_terminate:
            logger.info(f'Received a kill signal from {addr}:{port}.')
            terminate()

        # Job manager is sending heartbeats
        if mtype == messaging.msg_send_heart:
            logger.debug(f'Received heartbeat from {addr}:{port}')

        # Job manager is trying to send tasks to the task manager
        elif mtype == messaging.msg_send_task:
            # Two phase pull: test-try-pull
            while not tpool.Full():
                # Task pool is not full, start asking for data
                conn.WriteInt64(messaging.msg_send_more)
                # Write Data
                taskid = conn.ReadInt64(tm_recv_timeout)
                runid = conn.ReadInt64(tm_recv_timeout)
                tasksz = conn.ReadInt64(tm_recv_timeout)
                task = conn.Read(tasksz, tm_recv_timeout)
                logger.info('Received task {} from {}:{}.'.format(taskid, addr, port))

                # Try enqueue the received task
                if not tpool.Put(taskid, runid, task):
                    # For some reason the pool got full in between
                    # (shouldn't happen)
                    logger.warning('Rejecting task %d because the pool is full!', taskid)
                    conn.WriteInt64(messaging.msg_send_rjct)

            # Task pool is full, stop receiving tasks
            conn.WriteInt64(messaging.msg_send_full)

        # Job manager is querying the results of the completed tasks
        elif mtype == messaging.msg_read_result:
            taskid = None
            try:
                # Dequeue completed tasks until cqueue fires
                # an Empty exception
                while True:
                    # Pop the task
                    taskid, runid, r, res = cqueue.get_nowait()

                    logger.info('Sending task {} to committer {}:{}...'.format(taskid, addr, port))

                    # Send the task
                    conn.WriteInt64(taskid)
                    conn.WriteInt64(runid)
                    conn.WriteInt64(r)
                    if res is None:
                        conn.WriteInt64(0)
                    else:
                        conn.WriteInt64(len(res))
                        conn.Write(res)

                    # Wait for the confirmation that the task has
                    # been received by the other side
                    ans = conn.ReadInt64(messaging.msg_read_result)
                    if ans != messaging.msg_read_result:
                        logger.warning('Unknown response received from {}:{} while committing task'.format(addr, port))
                        raise messaging.MessagingError()

                    taskid = None

            except queue.Empty:
                # Finish the response
                conn.WriteInt64(messaging.msg_read_empty)

            except:
                # Something went wrong while sending, put
                # the last task back in the queue
                if taskid is not None:
                    cqueue.put((taskid, runid, r, res))
                    logger.info('Task {} put back in the queue.'.format(taskid))
                pass

        elif mtype == messaging.msg_cd_query_metrics_list:
            metrics = metrics.get_metrics()
            metrics = json.dumps(metrics)
            conn.WriteString(metrics)

        # Unknow message received or a wrong sized packet could be trashing
        # the buffer, don't do anything
        else:
            logger.warning(f"Unknown message received '{mtype}'!")

    except messaging.SocketClosed:
        logger.debug(f'Connection to {addr}:{port} closed from the other side.')

    except socket.timeout:
        logger.warning(f'Connection to {addr}:{port} timed out!')

    except:
        logger.warning(f'Error occurred while reading request from {addr}:{port}!')
        log_lines(traceback.format_exc(), logging.debug)

    conn.Close()
    logger.debug(f'Connection to {addr}:{port} closed.')


###############################################################################
# Initializer routine for the worker
###############################################################################
def initializer(cqueue, job, metrics, argv, active_workers, timeout):
    logger.info('Initializing worker...')
    return job.spits_worker_new(argv)


###############################################################################
# Worker routine
###############################################################################
TASKS_PROCESSED = 0
def worker(state, taskid, runid, task, cqueue, job, metrics: MetricManager, argv, active_workers, timeout):
    global TASKS_PROCESSED
    timeout.reset()
    active_workers.inc()
    logger.info('Processing task %d from job %d...', taskid, runid)

    # Execute the task using the job module
    start_time = time.time()
    r, res, ctx = job.spits_worker_run(state, task, taskid)
    task_time = time.time() - start_time

    logger.info('Task %d processed.', taskid)

    if res is None:
        logger.error('Task %d did not push any result!', taskid)
        return

    if ctx != taskid:
        logger.error('Context verification failed for task %d!', taskid)
        return

    # Enqueue the result
    TASKS_PROCESSED += 1
    metrics.set_metric("tasks_processed", TASKS_PROCESSED)
    metrics.set_metric("task_time", task_time)
    cqueue.put((taskid, runid, r, res[0]))
    active_workers.dec()


###############################################################################
# Run routine
###############################################################################


class AtomicInc(object):
    def __init__(self, value=0):
        self.value = value
        self.lock = Lock()

    def inc(self):
        self.lock.acquire()
        self.value += 1
        self.lock.release()

    def dec(self):
        self.lock.acquire()
        self.value -= 1
        self.lock.release()

    def get(self):
        return self.value


class App(object):
    def __init__(self):
        global spits_binary, spits_binary_args, tm_nw, tm_overfill, tm_mode, \
            tm_addr, tm_port, METRICS
        self.margv = [spits_binary] + spits_binary_args
        self.timeout = Timeout(tm_timeout, self.timeout_exit)
        self.job = JobBinary(spits_binary)
        self.metrics = MetricManager(self.job, buffer_size=10)
        METRICS = self.metrics
        self.job.metrics = Pointer(self.metrics.metric_manager)
        self.cqueue = queue.Queue()
        self.active_workers = AtomicInc()
        data = (self.cqueue, self.job, self.metrics, self.margv, self.active_workers, self.timeout)
        self.tpool = TaskPool(tm_nw, tm_overfill, initializer, worker, data)
        self.server = Listener(tm_mode, tm_addr, tm_port, server_callback,
                               (self.job, self.metrics, self.tpool, self.cqueue, self.timeout))

    def run(self):
        global tm_spits_profile_buffer_size, tm_nw, tm_perf_rinterv, \
            tm_perf_subsamp, tm_profiling, tm_announce, metrics_file, \
            tm_hostname
        # self.job.spits_metric_new(tm_spits_profile_buffer_size)
        # self.job.spits_set_metric_int("created_time", int(time.time()))
        # Enable perf module
        if tm_profiling:
            PerfModule(make_uid(), tm_nw, tm_perf_rinterv, tm_perf_subsamp)

        self.timeout.reset()
        logger.info('Starting workers...')
        self.tpool.start()
        logger.info('Starting network listener...')
        self.server.Start()
        if tm_hostname:
            addr = f"{tm_hostname}:{self.server.port}"
        else:
            addr = self.server.GetConnectableAddr()
        if tm_announce == config.announce_cat_nodes:
            announce_cat(addr)
        elif tm_announce == config.announce_file:
            announce_file(addr)
        logger.info(f'Announcing: {addr}')
        logger.info('Waiting for work...')
        #self.job.spits_set_metric_string("tm_start_time", datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f"))
        self.server.Join()
        self.server.Stop()
        # self.job.spits_metric_finish()

    def timeout_exit(self):
        if self.tpool.Empty() and self.active_workers.get() <= 0:
            logger.error('Task Manager exited due to timeout')
            self.server.Stop()
            sys.exit(1)
        else:
            return True


###############################################################################
# Main routine
###############################################################################
def main(argv):
    global tm_verbosity, tm_log_file
    parse_global_config(argv)
    setup_log(tm_verbosity, tm_log_file)
    logger.debug('Hello!')
    App().run()
    logger.debug('Bye!')
    terminate()


###############################################################################
# Entry point
###############################################################################
if __name__ == '__main__':
    main(sys.argv[1:])
