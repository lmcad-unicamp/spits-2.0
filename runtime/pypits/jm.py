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

import datetime
import json
import logging
import os
import sys
import threading
import time
import traceback
import argparse
import uuid

from libspits import JobBinary, SimpleEndpoint, get_logger, setup_log
from libspits import Listener
from libspits import PerfModule
from libspits import Pointer
from libspits import log_lines
from libspits import make_uid
from libspits import memstat
from libspits import messaging, config

# Global configuration parameters
jm_killtms = None       # Kill task managers after execution
jm_log_file = None      # Output file for logging
jm_verbosity = 0        # Verbosity level for logging
jm_heart_timeout = None # Timeout for heartbeat response
jm_conn_timeout = None  # Socket connect timeout
jm_recv_timeout = None  # Socket receive timeout
jm_send_timeout = None  # Socket send timeout
jm_send_backoff = None  # Job Manager delay between sending tasks
jm_recv_backoff = None  # Job Manager delay between sending tasks
jm_memstat = None  # 1 to display memory statistics
jm_profiling = None  # 1 to enable profiling
jm_perf_rinterv = None  # Profiling report interval (seconds)
jm_perf_subsamp = None  # Number of samples collected between report intervals
jm_jobid = None
jm_name = None
jm_port = 0
jm_working_dir = None
spits_binary = ''
spits_binary_args = []

# Job Manager Metrics
jm_spits_profile_buffer_size = None  # Circular Buffer size for the spits application
jm_counter_tasks_generated = 0
jm_counter_run_iterations = 0
jm_counter_tasks_sent = 0
jm_counter_tasks_replicated = 0

# Commiter Metrics
co_counter_tasks_commited = 0
co_counter_results_received = 0
co_counter_results_discarded = 0
co_counter_tasks_error = 0

spits_running = True

logger = get_logger(__name__)


###############################################################################
# Parse global configuration
###############################################################################
def parse_global_config(arguments):
    """ Parse the command-line arguments to global variables
    """
    global jm_killtms, jm_log_file, jm_verbosity, jm_heart_timeout, \
        jm_conn_timeout, jm_recv_timeout, jm_send_timeout, jm_send_backoff, \
        jm_recv_backoff, jm_memstat, jm_profiling, jm_perf_rinterv, \
        jm_perf_subsamp, jm_jobid, \
        jm_spits_profile_buffer_size, jm_name, jm_port, jm_working_dir, \
        spits_binary, spits_binary_args

    parser = argparse.ArgumentParser(description="SPITS Job Manager runtime")
    parser.add_argument('binary', metavar='PATH', type=str,
                        help='Path to the SPITS binary')
    parser.add_argument('binary_args', metavar='ARGS', type=str,
                        nargs=argparse.REMAINDER,
                        help='SPITS binary arguments')
    parser.add_argument('--killtms', action='store_true', default=False,
                        help="Kill active task managers when job manager exits "
                             "(default: %(default)s)")
    parser.add_argument('--log', action='store', type=str, metavar='PATH',
                        help="Redirect log messages to a file")
    parser.add_argument('--cwd', action='store', type=str, metavar='PATH',
                        help="Set the current working directory for the binary")
    parser.add_argument('--verbose', action='store', metavar='LEVEL',
                        type=int, default=config.def_verbosity,
                        help="Verbosity level (default: %(default)s)")
    parser.add_argument('--htimeout', action='store', metavar='TIME',
                        type=int, default=config.def_heart_timeout,
                        help="Timeout for heartbeat response "
                             "(default: %(default)s)")
    parser.add_argument('--ctimeout', action='store', metavar='TIME',
                        type=int, default=config.def_connection_timeout,
                        help="Socket connect timeout (default: %(default)s)")
    parser.add_argument('--rtimeout', action='store', metavar='TIME',
                        type=int, default=config.def_receive_timeout,
                        help="Socket receive timeout (default: %(default)s)")
    parser.add_argument('--stimeout', action='store', metavar='TIME',
                        type=int, default=config.def_send_timeout,
                        help="Socket send timeout (default: %(default)s)")
    parser.add_argument('--rbackoff', action='store', metavar='TIME',
                        type=int, default=config.recv_backoff,
                        help="Job Manager delay between receiving tasks "
                             "(default: %(default)s)")
    parser.add_argument('--sbackoff', action='store', metavar='TIME',
                        type=int, default=config.send_backoff,
                        help="Job Manager delay between sending tasks "
                             "(default: %(default)s)")
    parser.add_argument('--memstat', action='store_true', default=False,
                        help="Display memory statistics (default: %(default)s)")
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
    parser.add_argument('--jobid', action='store', metavar='ID',
                        type=str, default='', help="ID of the job")
    parser.add_argument('--name', action='store', type=str, default='',
                        help="Name for the Job Manager")
    parser.add_argument('--metric-buffer', action='store', type=int, default=10,
                        help="Number of elements for each metric buffer "
                             "(default: %(default)s)")
    parser.add_argument('--port', action='store', type=int,
                        default=config.def_spits_jm_port,
                        help="Job manager port to that socket will listen to "
                             "(default: %(default)s)")

    args = parser.parse_args(arguments)
    print(f"ARGS: {args}")
    spits_binary = args.binary
    if not os.path.isfile(spits_binary):
        raise ValueError(f"Invalid spits binary at: {spits_binary}")
    spits_binary_args = args.binary_args
    jm_killtms = args.killtms
    jm_log_file = args.log
    jm_working_dir = args.cwd
    if jm_working_dir is not None and not os.path.isdir(jm_working_dir):
        raise ValueError(f"Invalid working directory: {jm_working_dir}")
    jm_verbosity = args.verbose
    jm_heart_timeout = args.htimeout
    jm_conn_timeout = args.ctimeout
    jm_recv_timeout = args.rtimeout
    jm_send_timeout = args.stimeout
    jm_recv_backoff = args.rbackoff
    jm_send_backoff = args.sbackoff
    jm_memstat = args.memstat
    jm_profiling = args.profile
    jm_perf_rinterv = args.perf_interval
    jm_perf_subsamp = args.perf_subsamp
    jm_jobid = args.jobid
    jm_name = args.name or f'Job-Manager-{str(uuid.uuid4()).replace("-", "")}'
    jm_spits_profile_buffer_size = args.metric_buffer
    jm_port = args.port

    # jm_killtms = as_bool(argdict.get('killtms', True))
    # jm_log_file = argdict.get('log', None)
    # jm_working_dir = argdict.get('cwd', None)
    # jm_verbosity = as_int(argdict.get('verbose', logging.INFO // 10)) * 10
    # jm_heart_timeout = as_float(argdict.get('htimeout', config.def_heart_timeout))
    # jm_conn_timeout = as_float(argdict.get('ctimeout', config.def_connection_timeout))
    # jm_recv_timeout = as_float(argdict.get('rtimeout', config.def_receive_timeout))
    # jm_send_timeout = as_float(argdict.get('stimeout', config.def_send_timeout))
    # jm_recv_backoff = as_float(argdict.get('rbackoff', config.recv_backoff))
    # jm_send_backoff = as_float(argdict.get('sbackoff', config.send_backoff))
    # jm_memstat = as_int(argdict.get('memstat', 0))
    # jm_profiling = as_int(argdict.get('profiling', 0))
    # jm_perf_rinterv = as_int(argdict.get('rinterv', 60))
    # jm_perf_subsamp = as_int(argdict.get('subsamp', 12))
    # jm_jobid = argdict.get('jobid', '')
    # jm_name = argdict.get('jmname', 'JobManager-{}'.format(os.getpid()))
    # jm_spits_profile_buffer_size = as_int(argdict.get('profile-buffer', 10))
    # jm_port = as_int(argdict.get('jmport', config.def_spits_jm_port))


###############################################################################
# Abort the aplication with message
###############################################################################
def abort(error: str) -> None:
    """ Abort the execution of the program with a message

    :param error: Error message
    :type error: str
    """
    global spits_running

    spits_running = False
    logger.critical(error)
    exit(1)


###############################################################################
# Parse the definition of a proxy
###############################################################################
def parse_proxy(proxy_string: str) -> dict:
    """ Parse a proxy string

    :param proxy_string: Proxy string to be parsed. Ex "proxy name protocol:address:port"
    :type proxy_string: str
    :return: dict containing {'name': proxy_name, 'protocol': xxx, 'address': ip_address, 'port': port}
    :rtype: dict
    """
    proxy_string = proxy_string.split()
    if len(proxy_string) != 3:
        raise Exception('Invalid proxy string {}'.format(proxy_string))

    logger.debug('Proxy {}'.format(proxy_string[1]))
    name = proxy_string[1]
    gate = proxy_string[2].split(':')
    protocol = gate[0]
    address = gate[1]
    port = int(gate[2])
    return {'name': name, 'protocol': protocol, 'address': address, 'port': port}


###############################################################################
# Parse the definition of a compute node
###############################################################################
def parse_node(node_string: str, proxies: list) -> tuple:
    """ Parse a node string (format: 'node hostname:port' or 'node hostname:port through proxy_name' )

    :param node_string: Node string to be parsed
    :type node_string: str
    :param proxies: List of proxies dicts (format: [{'name': proxy_name, 'protocol': xxx, 'address': ip_address, 'port': port}, ...])
    :type proxies: list
    :return: Tuple containing the name (str) and a non initialized SimpleEndpoint connection to the node.
    :rtype: tuple(str, SimpleEndpoint)
    """
    nodes_commands = node_string.split()
    if len(nodes_commands) < 2:
        raise Exception("Invalid node {}".format(node_string))

    logger.debug('Node {}'.format(nodes_commands[1]))
    name = nodes_commands[1]
    addr = nodes_commands[1].split(':')[0]
    port = int(nodes_commands[1].split(':')[1])

    # Just 'node ip:port'. Create a simple Endpoint
    if len(nodes_commands) == 2:
        return name, SimpleEndpoint(addr, port)

    # Endpoint behind a proxy 'node ip:port through proxyname'
    elif len(nodes_commands) == 4:
        if nodes_commands[2] != 'through':
            raise Exception("Unknown proxy command format for: {}".format(node_string))

        proxy_name = nodes_commands[3]
        proxy = next([proxy for proxy in proxies if proxy['name'] == proxy_name], None)
        if proxy is None:
            raise Exception("Unknown proxy with name: '{}'".format(proxy_name))

        # Proxies are not supported yet...
        raise Exception("Node '{}' is behind a proxy and is not supported yet...".format(node_string))

    # Unknown command format
    raise Exception("Unknown command format for node: '{}'".format(node_string))


def add_node(ipaddr: str, port: int, dirname: str = 'nodes') -> bool:
    """ Add a worker node to nodes directory

    :param ipaddr: Address of node
    :type ipaddr: str
    :param port: Port used by the node
    :type port: int
    :param dirname: Nodes directory to be used. The default directory is 'nodes'
    :type dirname: str
    :return: True on success or false otherwise
    :rtype: bool
    """
    try:
        filename = os.path.join(dirname, "{}-{}".format(ipaddr, port))
        with open(filename, 'w') as f:
            f.write("node {}:{}".format(ipaddr, port))
        return True
    except:
        logger.error("Error adding node to file {}-{} from {}".format(ipaddr, port, dirname))
        logger.error(traceback.format_exc())

    return False


def remove_node(ipaddr: str, port: int, dirname: str = 'nodes') -> bool:
    """ Remove a worker node from the nodes directory

    :param ipaddr: Address of node
    :type ipaddr: str
    :param port: Port used by the node
    :type port: int
    :param dirname: Nodes directory to be used. The default directory is 'nodes'
    :type dirname: str
    :return: True on success or false otherwise
    :rtype: bool
    """
    try:
        filename = os.path.join(dirname, "{}-{}".format(ipaddr, port))
        os.unlink(filename)
        return True

    except:
        logger.error("Error removing node from file {}-{} in directory {}".format(ipaddr, port, dirname))
        logger.error(traceback.format_exc())

    return False


def list_nodes(dirname: str = 'nodes') -> dict:
    """ List current nodes

    :param dirname: Nodes directory to be used. The default directory is 'nodes'
    :type dirname: str
    :return: Dictionary with {"nodes": [{"host": "xxx.xxx.xxx.xxx", "port": yyyyy}, ...]}
    :
    """
    try:
        tms = load_tm_list_from_dir()
        tms_dict_list = dict(nodes=[{
                "host": name,
                "port": endpoint.port}
            for name, endpoint in tms])
        return tms_dict_list
    except:
        logger.error("Error listing nodes!")
        logger.error(traceback.format_exc())

    return {}


###############################################################################
# Load the list of task managers from a file
###############################################################################
def load_tm_list_from_file(filename: str = None) -> dict:
    """ Parses and loads task manager nodes from a file

    :param filename: File to be parsed. If None is passed, the file name 'nodes.txt' is used by default
    :type filename: str
    :return: A dictionary with the all the nodes from the file. The name of the node (ip address) is the key and a
             SimpleEndpoint is value (name, SimpleEndpoint). The endpoint contains the ip and port from the node.
    :rtype: dict
    """
    # Override the filename if it is empty
    if filename is None:
        filename = os.path.join('.', 'nodes.txt')
    logger.debug('Loading task manager list from file {}...'.format(filename))
    tms = {}

    # Read all lines
    try:
        with open(filename, 'rt') as file:
            lines = file.readlines()

        proxies_list = [parse_proxy(line.strip()) for line in lines if line[0:5] == 'proxy']
        tms_list = [parse_node(line.strip(), proxies_list) for line in lines if line[0:4] == 'node']
        tms = {name: endpoint for name, endpoint in tms_list if name is not None}

    except:
        logger.warning('Error loading the list of task managers from the file {}'.format(filename))

    return tms


###############################################################################
# Load the list of task managers from a file
###############################################################################
def load_tm_list_from_dir(dirname: str = 'nodes') -> dict:
    """ Loads task managers from a directory. For each file in the directory it will call the 'load_tm_list_from_file' and load a node file

    :param dirname: Nodes directory to be used. The default directory is 'nodes'
    :type dirname: str
    :return: A dictionary with the all the nodes from the files of the directory. The name of the node (ip address) is the key and a SimpleEndpoint is value (name, SimpleEndpoint). The endpoint contains the ip and port from the node.
    :rtype: dict
    """

    logger.debug('Loading task manager list from directory {}...'.format(dirname))
    tms = {}
    # Read all files
    try:
        for f in os.listdir(dirname):
            f = os.path.join(dirname, f)
            if not os.path.isfile(f):
                continue
            tms.update(load_tm_list_from_file(f))
    except:
        logger.warning('Error loading the list of task managers from the directory {}'.format(dirname))
        return {}

    return tms


###############################################################################
# Load the list of task managers from a file
###############################################################################
def load_tm_list():
    """ Loads task manager list from file 'nodes.txt' and for 'nodes' directory

    :return: A dictionary with the all the nodes parsed. The name of the node (ip address) is the key and a SimpleEndpoint is value. The endpoint contains the ip and port from the node.
    :rtype: dict
    """
    tms = load_tm_list_from_file()
    tms.update(load_tm_list_from_dir())
    logger.debug('Loaded {} task manager nodes.'.format(len(tms)))
    return tms


###############################################################################
# Exchange messages with an endpoint to begin pushing tasks
###############################################################################
def setup_endpoint_for_pushing(e: SimpleEndpoint) -> bool:
    """ Establishes a endpoint connection with a task manager and asks if its possible to send more tasks
        Exchange messages with an endpoint to begin pushing tasks

    :param e: Simple Endpoint to task manager node
    :type e: SimpleEndpoint
    :return: True if more tasks can be send, false otherwise
    *  If True is returned a connection stay open
    *  If False is returned the connection is closed
    :rtype: bool
    """
    global jm_conn_timeout, jm_jobid, jm_recv_timeout

    try:
        # Try to connect to a task manager
        e.Open(jm_conn_timeout)
    except:
        # Problem connecting to the task manager.
        # Because this is a connection event. Make it a debug rather than a warning
        logger.debug('Error connecting to task manager at {}:{}!'.format(e.address, e.port))
        log_lines(traceback.format_exc(), logging.debug)
        e.Close()
        return False

    try:
        # Send the job identifier, and verify job id of the answer
        e.WriteString(jm_jobid)
        received_jobid = e.ReadString(jm_recv_timeout)

        if jm_jobid != received_jobid:
            logger.error('Job Id mismatch from {}:{}! Self: {}, task manager: {}!'.format(
                e.address, e.port, jm_jobid, received_jobid))
            e.Close()
            return False

        e.WriteInt64(messaging.msg_send_task)

        # Wait for a response
        response = e.ReadInt64(jm_recv_timeout)

        # Task mananger is full
        if response == messaging.msg_send_full:
            logger.debug('Task manager at {}:{} is full.'.format(e.address, e.port))

        # Task Manager is not full, continue to push tasks to the TM
        elif response == messaging.msg_send_more:
            return True

        # The task manager is not replying as expected
        else:
            logger.error('Unknown response from the task manager!')

    except:
        # Problem connecting to the task manager
        logger.warning('Error connecting to task manager at {}:{}!'.format(e.address, e.port))
        log_lines(traceback.format_exc(), logging.debug)

    e.Close()
    return False


###############################################################################
# Exchange messages with an endpoint to begin reading results
###############################################################################
def setup_endpoint_for_pulling(e: SimpleEndpoint) -> bool:
    """ Exchange messages with an endpoint to begin reading results

    :param e: Simple Endpoint to task manager node
    :type e: SimpleEndpoint
    :return: True if it is possible to read results from task manager and false otherwise
    *  If True is returned a connection stay open
    *  If False is returned the connection is closed
    :rtype: bool
    """
    global jm_conn_timeout, jm_jobid, jm_recv_timeout

    try:
        # Try to connect to a task manager
        e.Open(jm_conn_timeout)
    except:
        # Problem connecting to the task manager
        # Because this is a connection event,
        # make it a debug rather than a warning
        logger.debug('Error connecting to task manager at {}:{}!'.format(e.address, e.port))
        log_lines(traceback.format_exc(), logging.debug)
        e.Close()
        return False

    try:
        # Send the job identifier
        e.WriteString(jm_jobid)

        # Verify job id of the answer
        jobid = e.ReadString(jm_recv_timeout)

        if jm_jobid != jobid:
            logger.error('Job Id mismatch from {}:{}! Self: {}, task manager: {}!'.format(
                e.address, e.port, jm_jobid, jobid))
            e.Close()
            return False

        # Ask if it is possible to send tasks
        e.WriteInt64(messaging.msg_read_result)
        return True

    except:
        # Problem connecting to the task manager
        logger.warning('Error connecting to task manager at {}:{}!'.format(e.address, e.port))
        log_lines(traceback.format_exc(), logging.debug)

    e.Close()
    return False


###############################################################################
# Push tasks while the task manager is not full
###############################################################################
def push_tasks(job: JobBinary, runid: int, jm: Pointer, tm: SimpleEndpoint, taskid: int, task: Pointer, tasklist: dict,
               completed: list) -> tuple:
    """ Push tasks to a task manager while the it is not full

    :param job: The SPITS job binary object to interact with the binary application via C code
    :type job: JobBinary
    :param runid:
    :param jm: Pointer to a Job Manager instance, generated with 'spits_job_manager_new'
    :type jm: Pointer
    :param tm: Task Manager Endpoint for communication
    :type tm: SimpleEndpoint
    :param taskid: Task Identifier
    :type taskid: int
    :param task: The actual task
    :type task: Pointer
    :type tasklist: dict
    :param tasklist: A dict of tuples with generated tasks. The key is the task ID and value is a tuple which contains:
    * [0]: 0 if the task is not completed and 1 if task is already completed by some worker
    * [1]: The task
    :param completed: Variable indicating that all tasks were generated
    :type completed: list of bool
    :rtype: tuple
    :return: A tuple with 4 field:
    * [0]: True if there is no more task to generate or false otherwise
    * [1]: The task ID
    * [2]: The task or None
    * [3]: The successfully sent task list
    """
    global jm_counter_tasks_generated, jm_counter_tasks_sent, jm_counter_tasks_replicated, jm_recv_timeout

    # Keep pushing until finished or the task manager is full
    sent = []

    while True:
        if task is None:
            # Avoid calling next_task after it's finished
            if completed:
                logger.debug('There are no new tasks to generate.')
                return True, 0, None, sent

            # Only get a task if the last one was already sent.
            newtaskid = taskid + 1
            # Create a new task with the newtaskid
            return_code, newtask, ctx = job.spits_job_manager_next_task(jm, newtaskid)

            # Exit if done
            if return_code == 0:
                return True, 0, None, sent

            # Error generating task, return the context
            if newtask is None:
                logger.error('Task {} was not pushed!'.format(newtaskid))
                return False, taskid, task, sent

            if ctx != newtaskid:
                logger.error('Context verification failed for task {}!'.format(newtaskid))
                return False, taskid, task, sent

            taskid = newtaskid
            # Get the task
            task = newtask[0]
            # Add the generated task to the tasklist
            tasklist[taskid] = (0, task)
            # Increment the number of successfully generated tasks
            jm_counter_tasks_generated += 1
            job.spits_set_metric_int("tasks_generated", jm_counter_tasks_generated)
            logger.debug(
                'Generated task {} with payload size of {} bytes.'.format(taskid, len(task) if task is not None else 0))
        # else:
        #    duplicated = True

        try:
            logger.debug('Pushing task {} to the Task Manager at {}:{}..'.format(taskid, tm.address, tm.port))

            # Send the task to the active task manager. Send (taskid, runid, tasksize, task)
            tm.WriteInt64(taskid)
            tm.WriteInt64(runid)
            if task is None:
                tm.WriteInt64(0)
            else:
                tm.WriteInt64(len(task))
                tm.Write(task)

            # Wait for a response (may be reject/full/send_more)
            response = tm.ReadInt64(jm_recv_timeout)
            # Increment the sent tasks
            jm_counter_tasks_sent += 1
            job.spits_set_metric_int("tasks_sent", jm_counter_tasks_sent)

            # Task was sent, but the task manager is now full. Stop sending for a while...
            if response == messaging.msg_send_full:
                sent.append((taskid, task))
                task = None
                break

            # Task manager is not full and more tasks can be sent!
            elif response == messaging.msg_send_more:
                # Continue pushing tasks
                sent.append((taskid, task))
                task = None
                pass

            # Task was rejected by the task manager, this is not
            # predicted for a model where just one task manager
            # pushes tasks, exit the task loop
            elif response == messaging.msg_send_rjct:
                logger.warning('Task manager at {}:{} rejected task {}'.format(tm.address, tm.port, taskid))
                break

            # The task manager is not replying as expected
            else:
                logger.error('Unknown response from the task manager!')
                break
        except:
            # Something went wrong with the connection,
            # try with another task manager
            logger.error('Error pushing tasks to task manager at {}:{}!'.format(tm.address, tm.port))
            log_lines(traceback.format_exc(), logging.debug)
            break

    return False, taskid, task, sent


###############################################################################
# Read and commit tasks while the task manager is not empty
###############################################################################
def commit_tasks(job, runid, co, tm, tasklist, completed):
    global co_counter_tasks_commited, co_counter_results_received, co_counter_results_discarded, co_counter_tasks_error
    # Keep pulling until finished or the task manager is full
    n_errors = 0
    co_counter_results_received += len(tasklist)
    while True:
        try:
            # Pull the task from the active task manager
            taskid = tm.ReadInt64(jm_recv_timeout)

            if taskid == messaging.msg_read_empty:
                # No more task to receive
                return

            # Read the run id
            taskrunid = tm.ReadInt64(jm_recv_timeout)

            # Read the rest of the task
            r = tm.ReadInt64(jm_recv_timeout)
            ressz = tm.ReadInt64(jm_recv_timeout)
            res = tm.Read(ressz, jm_recv_timeout)

            # Tell the task manager that the task was received
            tm.WriteInt64(messaging.msg_read_result)

            # Warning, exceptions after this line may cause task loss
            # if not handled properly!!

            if r != 0:
                n_errors += 1
                co_counter_tasks_error += 1
                if r == messaging.res_module_error:
                    logger.error('the remote worker crashed while ' +
                                  'executing task %d!', r)
                else:
                    logger.error('The task %d was not successfully executed, ' +
                                  'worker returned %d!', taskid, r)
                job.spits_set_metric_int("results_error", co_counter_tasks_error)

            if taskrunid < runid:
                logger.debug('The task %d is from the previous run %d ' +
                              'and will be ignored!', taskid, taskrunid)
                co_counter_results_discarded += 1
                job.spits_set_metric_int("results_discarded", co_counter_results_discarded)
                continue

            if taskrunid > runid:
                logger.error('Received task %d from a future run %d!',
                              taskid, taskrunid)
                co_counter_results_discarded += 1
                job.spits_set_metric_int("results_discarded", co_counter_results_discarded)
                continue

            # Validated completed task

            c = completed.get(taskid, (None, None))
            if c[0] != None:
                # This may happen with the fault tolerance system. This may
                # lead to tasks being put in the tasklist by the job manager
                # while being committed. The tasklist must be constantly
                # sanitized.
                logger.warning('The task %d was received more than once ' +
                                'and will not be committed again!',
                                taskid)
                # Removed the completed task from the tasklist
                tasklist.pop(taskid, (None, None))
                co_counter_results_discarded += 1
                job.spits_set_metric_int("results_discarded", co_counter_results_discarded)
                continue

            # Remove it from the tasklist

            p = tasklist.pop(taskid, (None, None))
            if p[0] == None and c[0] == None:
                # The task was not already completed and was not scheduled
                # to be executed, this is serious problem!
                logger.error('The task %d was not in the working list!',
                              taskid)

            r2 = job.spits_committer_commit_pit(co, res)

            if r2 != 0:
                logger.error('The task %d was not successfully committed, ' +
                              'committer returned %d', taskid, r2)
                co_counter_tasks_error += 1
                job.spits_set_metric_int("results_error", co_counter_tasks_error)
            else:
                co_counter_tasks_commited += 1
                job.spits_set_metric_int("tasks_commited", co_counter_tasks_commited)

            # Add completed task to list
            completed[taskid] = (r, r2)

        except:
            # Something went wrong with the connection,
            # try with another task manager
            break
    if n_errors > 0:
        logger.warning('There were %d failed tasks' % (n_errors,))


def infinite_tmlist_generator():
    """ Iterates over TMs returned by the load_tm_list() method indefinitely.
    The result of a single iteration is a tuple containing (Finished, Name,
    TM), where Finished == True indicates if the currently listed  TMs
    finished. The next iteration will read the TMs again, setting Finished to
    False.

    Conditions:
        * Finished == True <=> (Name, TM) == (None, None)
        * Finished == False <=> (Name, TM) != (None, None)

    Example:
        for isEnd, name, tm in infinite_tmlist_generator():
            if not isEnd:
                do something with the task manager (name, tm)
            else:
                all tms where processed, you can do post processing here. The
                next iteration will set isEnd to True and start over again
    """
    tmlist = load_tm_list()
    while True:
        try:
            newtmlist = load_tm_list()
            if len(newtmlist) > 0:
                tmlist = newtmlist
            elif len(tmlist) > 0:
                logger.warning('New list of task managers is empty and will not be updated!')
        except:
            if len(tmlist) > 0:
                logger.warning('New list of task managers is empty and will not be updated!')
        for name, tm in tmlist.items():
            yield False, name, tm
        yield True, None, None


###############################################################################
# Heartbeat routine
###############################################################################
def heartbeat(finished):
    global jm_heart_timeout
    t_last = time.time()
    for isEnd, name, tm in infinite_tmlist_generator():
        if finished[0]:
            logger.debug('Stopping heartbeat thread...')
            return
        if isEnd:
            t_curr = time.time()
            elapsed = t_curr - t_last
            t_last = t_curr
            sleep_for = max(jm_heart_timeout - elapsed, 0)
            time.sleep(sleep_for)
        else:
            try:
                tm.Open(jm_heart_timeout)
            except:
                # Problem connecting to the task manager
                # Because this is a connection event,
                # make it a debug rather than a warning
                logger.debug('Error connecting to task manager at {}:{}!'.format(tm.address, tm.port))
                log_lines(traceback.format_exc(), logging.debug)
                tm.Close()
                continue
            try:
                # Send the job identifier
                tm.WriteString(jm_jobid)

                # Verify job id of the answer
                jobid = tm.ReadString(jm_recv_timeout)

                if jm_jobid != jobid:
                    logger.error('Job Id mismatch from {}:{}! Self: {}, task manager: {}!'.format(
                        tm.address, tm.port, jm_jobid, jobid))
                    tm.Close()
                    continue

                # Send the heartbeat
                tm.WriteInt64(messaging.msg_send_heart)
            except:
                logger.warning('Error connecting to task manager at {}:{}!'.format(tm.address, tm.port))
                log_lines(traceback.format_exc(), logging.debug)
            finally:
                tm.Close()


###############################################################################
# Job Manager routine
###############################################################################
def jobmanager(argv: list, job: JobBinary, runid: int, jm: Pointer, tasklist: dict, completed: list) -> None:
    """ Job Manager routine

    :param argv: Arguments from JobManager
    :type argv: list of str
    :param job: The SPITS job binary object to interact with the binary application via C code
    :type job: JobBinary
    :param runid: Run identifier for the Job Manager
    :type runid: int
    :param jm: Pointer to a Job Manager instance, generated with 'spits_job_manager_new'
    :type jm: Pointer
    :type tasklist: dict
    :param tasklist: A dict of tuples with generated tasks. The key is the task ID and value is a tuple which contains:
    * [0]: 0 if the task is not completed and 1 if task is already completed by some worker
    * [1]: The task
    :param completed: Variable indicating that all tasks were generated
    :type completed: list of bool
    """
    global jm_send_backoff, spits_running

    logger.info('Job manager running...')
    memstat.stats()

    # Load the list of nodes to connect to
    tmlist = load_tm_list()

    # Store some metadata
    submissions = []  # (taskid, submission time, [sent to])

    # Task generation loop
    taskid = 0
    task = None

    while spits_running:
        # Reload the list of task managers at each
        # run so new tms can be added on the fly
        try:
            newtmlist = load_tm_list()
            if len(newtmlist) > 0:
                tmlist = newtmlist
            elif len(tmlist) > 0:
                logger.warning('New list of task managers is empty and will not be updated!')
        except:
            logger.error('Failed parsing task manager list!')
            logger.error(traceback.format_exc())

        # (name, SimpleEndPoint)
        # Push tasks to each Task Manager until its full
        for name, tm in tmlist.items():
            logger.debug('Connecting to {}:{}...'.format(tm.address, tm.port))

            # Open the connection to the task manager and query if it is
            # possible to send data
            if not setup_endpoint_for_pushing(tm):
                finished = False
            else:
                logger.debug('Pushing tasks to {}:{}...'.format(tm.address, tm.port))

                # Task pushing loop. Send tasks to the task manager until its full
                memstat.stats()
                finished, taskid, task, sent = push_tasks(job, runid, jm, tm, taskid, task, tasklist, completed[0] == 1)

                # Add the sent tasks to the submission list
                submissions = submissions + sent

                # Close the connection with the task manager
                tm.Close()

                logger.debug('Finished pushing tasks to {}:{}. Sent {} tasks'.format(tm.address, tm.port, len(sent)))

            if finished and completed[0] == 0:
                # Tell everyone the task generation was completed
                logger.info('All tasks generated.')
                completed[0] = 1

            # Exit the job manager when done
            if len(tasklist) == 0 and completed[0] == 1:
                logger.debug('Job manager exiting...')
                return

            # Keep sending the uncommitted tasks
            # TODO: WARNING this will flood the system
            # with repeated tasks
            if finished and len(tasklist) > 0:
                if len(submissions) == 0:
                    logger.critical('The submission list is empty but the task list is not! Some tasks were lost!')

                # Select the oldest task that is not already completed
                while True:
                    taskid, task = submissions.pop(0)
                    if taskid in tasklist:
                        break

        # Remove the committed tasks from the submission list
        submissions = [x for x in submissions if x[0] in tasklist]
        time.sleep(jm_send_backoff)

    logger.info("Shutting down JobManager..")


###############################################################################
# Committer routine
###############################################################################
def committer(argv, job, runid, co, tasklist, completed):
    global spits_running
    logger.info('Committer running...')
    memstat.stats()

    # Load the list of nodes to connect to
    tmlist = load_tm_list()

    # Result pulling loop
    while spits_running:
        # Reload the list of task managers at each
        # run so new tms can be added on the fly
        try:
            newtmlist = load_tm_list()
            if len(newtmlist) > 0:
                tmlist = newtmlist
            elif len(tmlist) > 0:
                logger.warning('New list of task managers is ' +
                                'empty and will not be updated!')
        except:
            logger.error('Failed parsing task manager list!')

        for name, tm in tmlist.items():
            logger.debug('Connecting to %s:%d...', tm.address, tm.port)

            # Open the connection to the task manager and query if it is
            # possible to send data
            if not setup_endpoint_for_pulling(tm):
                continue

            logger.debug('Pulling tasks from %s:%d...', tm.address, tm.port)

            # Task pulling loop
            commit_tasks(job, runid, co, tm, tasklist, completed)
            memstat.stats()

            # Close the connection with the task manager
            tm.Close()

            logger.debug('Finished pulling tasks from %s:%d.',
                          tm.address, tm.port)

            if len(tasklist) == 0 and completed[0] == 1:
                logger.info('All tasks committed.')
                logger.debug('Committer exiting...')
                return

        # Refresh the tasklist
        for taskid in completed:
            tasklist.pop(taskid, 0)

        time.sleep(jm_recv_backoff)

    logger.info("Shutting down Comitter...")


###############################################################################
# Kill all task managers
###############################################################################
def killtms():
    logger.info('Killing task managers...')

    # Load the list of nodes to connect to
    tmlist = load_tm_list()

    for name, tm in tmlist.items():
        try:
            logger.debug('Connecting to %s:%d...', tm.address, tm.port)

            tm.Open(jm_conn_timeout)

            # Send the job identifier
            tm.WriteString(jm_jobid)

            # Read back the job id of the answer
            tm.ReadString(jm_recv_timeout)

            tm.WriteInt64(messaging.msg_terminate)
            tm.Close()
        except:
            # Problem connecting to the task manager
            logger.warning('Error connecting to task manager at %s:%d!',
                            tm.address, tm.port)
            log_lines(traceback.format_exc(), logging.debug)


# Get Connectable Address
# msg_cd_query_metrics_list -- OK
# msg_cd_query_metrics_last_values -- OK
# msg_cd_query_metrics_history
def server_callback(conn, addr, port, job):
    """ Server callback handler

    :param conn: Connection
    :type conn: SimpleEndpoint
    :param addr:
    :type addr:
    :param port:
    :type port:
    :param job:
    :type job:
    :return:
    :rtype:
    """
    global jm_recv_timeout
    logger.debug('Connected to {}:{}.'.format(addr, port))
    # print('Connected to {}:{}.'.format(addr, port))

    try:
        mtype = conn.ReadInt64(jm_recv_timeout)

        if mtype == messaging.msg_cd_query_state:
            state_json = dict(pid=os.getpid(), type="jobmanager", status="running")
            state_json = json.dumps(state_json)
            conn.WriteString(state_json if state_json is not None else "")

        elif mtype == messaging.msg_cd_query_metrics_list:
            metrics_list = job.spits_get_metrics_list()
            metrics_list = json.dumps(metrics_list)
            conn.WriteString(metrics_list if metrics_list is not None else "")

        elif mtype == messaging.msg_cd_query_metrics_last_values:
            # Metrics list JSON. Format: {"metrics": [{"name": "metric1"}, {"name": "metric2"}, ...]}
            metrics_list = conn.ReadString(jm_recv_timeout)
            metrics_list = json.loads(metrics_list)
            metrics_last_values = job.spits_get_metrics_last_values([metric['name'] for metric in metrics_list['metrics']])
            metrics_last_values = json.dumps(metrics_last_values)
            conn.WriteString(metrics_last_values if metrics_last_values is not None else "")

        elif mtype == messaging.msg_cd_query_metrics_history:
            pass

        elif mtype == messaging.msg_cd_nodes_append:
            # Format { "host": "xxx.xxx.xxx.xxx", "port": 0000 }
            node_string = conn.ReadString(jm_recv_timeout)
            node_json = json.loads(node_string)
            # Send 0 on success and 1 otherwise
            conn.WriteInt64(0 if add_node(node_json['host'], node_json['port']) else 1)

        elif mtype == messaging.msg_cd_nodes_list:
            # Send the list of nodes {"nodes": [{"host": "xxx.xxx.xxx.xxx", "port": yyyyy}, {}, ...]} or {}, on error
            conn.WriteString(json.dumps(list_nodes()))

        elif mtype == messaging.msg_cd_nodes_remove:
            # Format { "host": "xxx.xxx.xxx.xxx", "port": 0000 }
            node_string = conn.ReadString(jm_recv_timeout)
            node_json = json.loads(node_string)
            # Send 0 on success and 1 otherwise
            conn.WriteInt64(0 if remove_node(node_json['host'], node_json['port']) else 1)

        else:
            raise Exception("Don't know option: {}".format(mtype))

    except Exception:
        logger.error(traceback.format_exc())

    conn.Close()
    logger.debug('Connection to {}:{} closed.'.format(addr, port))


###############################################################################
# Run routine
###############################################################################
def run(argv, jobinfo, job, runid):
    # List of pending tasks
    global jm_spits_profile_buffer_size, jm_name
    memstat.stats()
    tasklist = {}

    # Keep an extra list of completed tasks
    completed = {0: 0}

    # Start the Job Manager
    logger.info("Starting job manager {} for job {}...".format(jm_name, runid))
    # Create the job manager from the job module
    jm = job.spits_job_manager_new(argv, jobinfo)
    jmthread = threading.Thread(target=jobmanager, args=(argv, job, runid, jm, tasklist, completed))
    jmthread.start()
    job.spits_set_metric_string("jm_start_time", datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f"))

    # Start the committer
    logger.info('Starting committer for job {}...'.format(runid))
    # Create the committer manager from the job module
    co = job.spits_committer_new(argv, jobinfo)
    cothread = threading.Thread(target=committer, args=(argv, job, runid, co, tasklist, completed))
    cothread.start()
    job.spits_set_metric_string("co_start_time", datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f"))

    # Wait for both threads
    jmthread.join()
    cothread.join()
    # server_listener.Join()

    # Commit the job
    logger.info('Committing Job...')
    r, res, ctx = job.spits_committer_commit_job(co, 0x12345678)
    logger.debug('Job committed.')

    # Finalize the job manager
    logger.debug('Finalizing Job Manager...')
    job.spits_job_manager_finalize(jm)

    # Finalize the committer
    logger.debug('Finalizing Committer...')
    job.spits_committer_finalize(co)
    memstat.stats()

    if res is None:
        logger.error('Job did not push any result!')
        return messaging.res_module_noans, None

    if ctx != 0x12345678:
        logger.error('Context verification failed for job!')
        return messaging.res_module_ctxer, None

    logger.debug('Job {} finished successfully.'.format(runid))

    return r, res[0]


###############################################################################
# Main routine
###############################################################################
def main(argv):
    # Print usage
    global spits_running, spits_binary, spits_binary_args, jm_verbosity, \
        jm_log_file
    parse_global_config(argv)

    # Setup logging
    setup_log(jm_verbosity, jm_log_file)
    logger.debug('Hello!')

    # Enable memory debugging
    if jm_memstat:
        memstat.enable()
    memstat.stats()

    # Enable perf module
    if jm_profiling:
        PerfModule(make_uid(), 0, jm_perf_rinterv, jm_perf_subsamp)

    # Load the module
    job = JobBinary(spits_binary, buffer_size=jm_spits_profile_buffer_size)

    # Create the metrics manager
    # job.spits_metric_new(jm_spits_profile_buffer_size)

    # Remove JM arguments when passing to the module
    margv = [spits_binary] + spits_binary_args

    # Keep a run identifier
    runid = [0]

    # Wrapper to include job module
    def run_wrapper(argv, jobinfo):
        runid[0] = runid[0] + 1
        return run(argv, jobinfo, job, runid[0])

    # Wrapper for the heartbeat
    finished = [False]

    # Start the heartbeat
    def heartbeat_wrapper():
        heartbeat(finished)

    threading.Thread(target=heartbeat_wrapper).start()

    # Start the server listener
    server_listener = Listener(
        config.mode_tcp, '0.0.0.0', jm_port, server_callback, (job,))
    server_listener.Start()

    # Run the module
    logger.info('Running module...')
    memstat.stats()
    r = job.spits_main(margv, run_wrapper)
    memstat.stats()

    # Stop the heartbeat thread
    finished[0] = True
    spits_running = False
    server_listener.Stop()

    # Kill the workers
    if jm_killtms:
        killtms()

    # Print final memory report
    memstat.stats()

    # Print metrics
    # metrics = job.spits_get_metrics_list()
    # metric_history = job.spits_get_metrics_history([(m['name'], 10) for m in metrics])
    # print(metric_history)

    # Finalize
    logger.debug('Bye!')
    sys.exit(r)


###############################################################################
# Entry point
###############################################################################
if __name__ == '__main__':
    main(sys.argv[1:])
