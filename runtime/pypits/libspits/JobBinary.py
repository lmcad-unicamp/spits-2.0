#!/usr/bin/env python

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
# FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

import struct
import ctypes
import os
import base64

from typing import Union
from libspits import Blob, Pointer


# TODO try-except around C calls


class MethodNotFound(Exception):
    pass


class JobBinary(object):
    """Class binding for the external job binary loaded by spits"""

    # Constructor
    def __init__(self, filename: str, buffer_size=100):
        """
        Constructor for class

        :param filename: Name of the Binary
        :type filename: str
        """
        filename = os.path.realpath(filename)
        self.filename = filename
        self.module = ctypes.CDLL(filename)

        # Create the c function for the runner callback
        self.crunner = ctypes.CFUNCTYPE(
            ctypes.c_int,
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_void_p,
            ctypes.c_longlong,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_longlong))

        # Create the c function for the pusher callback
        self.cpusher = ctypes.CFUNCTYPE(
            None,
            ctypes.c_void_p,
            ctypes.c_longlong,
            ctypes.c_void_p)

        c_context = ctypes.c_void_p

        # Create the return type for some functions, otherwise
        # ctypes will assume int. Also create the arguments
        # otherwise ctypes may assume wrong

        # Do not fail if any method cannot be found because
        # this code is shared between job manager and task managers,
        # so different agents may use different binaries with
        # different parts of the interface exposed

        # Main

        self.try_init_method('spits_main', ctypes.c_int, [
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_char_p),
            self.crunner
        ])

        # Job Manager

        self.try_init_method('spits_job_manager_new', ctypes.c_void_p, [
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_void_p,
            ctypes.c_longlong,
            ctypes.c_void_p
        ])

        self.try_init_method('spits_job_manager_next_task', ctypes.c_int, [
            ctypes.c_void_p,
            self.cpusher,
            c_context
        ])

        self.try_init_method('spits_job_manager_finalize', None, [
            ctypes.c_void_p
        ])

        # Worker

        self.try_init_method('spits_worker_new', ctypes.c_void_p, [
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_void_p
        ])

        self.try_init_method('spits_worker_run', ctypes.c_int, [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_longlong,
            self.cpusher,
            c_context
        ])

        self.try_init_method('spits_worker_finalize', None, [
            ctypes.c_void_p
        ])

        # Committer

        self.try_init_method('spits_committer_new', ctypes.c_void_p, [
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.c_void_p,
            ctypes.c_longlong,
            ctypes.c_void_p
        ])

        self.try_init_method('spits_committer_commit_pit', ctypes.c_int, [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_longlong
        ])

        self.try_init_method('spits_committer_commit_job', ctypes.c_int, [
            ctypes.c_void_p,
            self.cpusher,
            c_context
        ])

        self.try_init_method('spits_committer_finalize', None, [
            ctypes.c_void_p
        ])

        self.metrics = None

    def try_init_method(self, name: str, restype, argtypes):
        """Set the specified format for a method in the module,
        if it exists. Return true if it does exist."""
        if not hasattr(self.module, name):
            raise MethodNotFound(f'Invalid ctype method {name}')
        method = getattr(self.module, name)
        method.restype = restype
        method.argtypes = argtypes

    def c_argv(self, argv):
        # Encode the string to byte array
        argv = [x.encode('utf8') for x in argv]

        # Cast the C arguments
        cargc = ctypes.c_int(len(argv))
        cargv = (ctypes.c_char_p * len(argv))()
        cargv[:] = argv
        return cargc, cargv

    def bytes(self, it):
        try:
            if bytes != str:
                return bytes(it)
        except:
            pass
        return struct.pack('%db' % len(it), *it)

    def unbyte(self, s):
        try:
            if bytes != str:
                return s
        except:
            pass
        return struct.unpack('%db' % len(s), s)

    def to_c_array(self, it):
        # Cover the case where an empty array or list is passed
        if it == None or len(it) == 0:
            return ctypes.c_void_p(None), 0
        # Normal C allocation
        cit = (ctypes.c_byte * len(it))()
        cit[:] = self.unbyte(it)
        citsz = ctypes.c_longlong(len(it))
        return cit, citsz

    def to_py_array(self, v, sz):
        v = ctypes.cast(v, ctypes.POINTER(ctypes.c_byte))
        return self.bytes(v[0:sz])

    def spits_main(self, argv, runner):
        # Call the runner if the job does not have an initializer
        if not hasattr(self.module, 'spits_main'):
            return runner(argv, None)

        # Create an inner converter for the callback
        def run(argc, argv, pjobinfo, jobinfosize, data, size):
            # Convert argc/argv back to a string list
            pargv = [argv[i].decode('utf8') for i in range(0, argc)]
            # Create a data blob without copying the buffer because
            # the method will lock the runner until it ends, so the
            # pointer is always valid
            bjobinfo = Blob(jobinfosize, pjobinfo)

            # Run the runner code
            r, pdata = runner(pargv, bjobinfo)

            # Convert the data result to a C pointer/size
            if pdata == None:
                data[0] = None
                size[0] = 0
            else:
                cdata = (ctypes.c_byte * len(pdata))()
                cdata[:] = self.unbyte(pdata)
                data[0] = ctypes.cast(cdata, ctypes.c_void_p)
                size[0] = len(pdata)

            return r

        # Cast the C arguments
        cargc, cargv = self.c_argv(argv)
        crun = self.crunner(run)

        # Call the C function
        return self.module.spits_main(cargc, cargv, crun)

    def spits_job_manager_new(self, argv, jobinfo):
        # Cast the C arguments
        cargc, cargv = self.c_argv(argv)
        # Call the native method casting the result to a simple pointer
        return Pointer(self.module.spits_job_manager_new(
            cargc, cargv, jobinfo.get_pointer(), jobinfo.get_size(),
            self.metrics.get_value()))

    def spits_job_manager_next_task(self, user_data: Pointer,
                                    jmctx: Union[Pointer, int]) -> tuple:
        """ Generates a new task using the job manager next_task interface from the job manager instance

        :param user_data: Ctype Pointer to the Job Manager instance
        :type user_data: Pointer
        :param jmctx: Task context (usually the actual task ID)
        :type jmctx: int or Pointer
        :return: A tuple with 3 fields:
        * [0]: 0 if no more task to generate and 1 otherwise
        * [1]: 1-element tuple containing the new generated task (byte array), received from the callback
        * [2]: The task context
        :rtype: tuple
        """
        res = [None, None, None]
        # Get the native pointer to the user data
        p_user_data = user_data.get_value()

        # Create an inner converter for the callback
        def push(ctask, ctasksz, ctx):
            """ Task pusher callback. This function is called when a new task is generated and pushed from the JM binary
            The result is placed in the 'res' variable

            :param ctask: Generated Task
            :type ctask: Pointer
            :param ctasksz: Size of the task
            :type ctasksz: int
            :param ctx: Actual task context
            :type ctx: Pointer
            """
            # Thanks to python closures, the context is not
            # necessary, in any case, check for correctness
            res[1] = (self.to_py_array(ctask, ctasksz),)
            res[2] = ctx

        # Get the next task
        res[0] = self.module.spits_job_manager_next_task(p_user_data, self.cpusher(push), jmctx)
        # Return the pushed data along with the return code of the method
        return res

    def spits_job_manager_finalize(self, user_data: Pointer):
        # Get the native pointer to the user data
        p_user_data = user_data.get_value()
        # Optional function
        if not hasattr(self.module, 'spits_job_manager_finalize'):
            return
        # Is expected that the framework will not mess with the
        # value inside user_data so its ctype will remain unchanged
        return self.module.spits_job_manager_finalize(p_user_data)

    def spits_worker_new(self, argv):
        # Cast the C arguments
        cargc, cargv = self.c_argv(argv)
        # Call the native method casting the result to a simple pointer
        return Pointer(self.module.spits_worker_new(
            cargc, cargv, self.metrics.get_value()))

    def spits_worker_run(self, user_data, task, taskctx):
        res = [None, None, None]
        # Get the native pointer to the user data
        p_user_data = user_data.get_value()
        # Create the pointer to task and task size
        ctask, ctasksz = self.to_c_array(task)

        # Create an inner converter for the callback
        def push(cres, cressz, ctx):
            # Thanks to python closures, the context is not
            # necessary, in any case, check for correctness
            res[1] = (self.to_py_array(cres, cressz),)
            res[2] = ctx

        # Run the task
        res[0] = self.module.spits_worker_run(p_user_data, ctask,
                                              ctasksz, self.cpusher(push), taskctx)
        # Return the pushed data along with the return code of the method
        return res

    def spits_worker_finalize(self, user_data):
        # Get the native pointer to the user data
        p_user_data = user_data.get_value()
        # Optional function
        if not hasattr(self.module, 'spits_worker_finalize'):
            return
        # Is expected that the framework will not mess with the
        # value inside user_data so its ctype will remain unchanged
        return self.module.spits_worker_finalize(p_user_data)

    def spits_committer_new(self, argv, jobinfo):
        # Cast the C arguments
        cargc, cargv = self.c_argv(argv)
        # Call the native method casting the result to a simple pointer
        return Pointer(self.module.spits_committer_new(cargc, cargv,
                                                       jobinfo.get_pointer(), jobinfo.get_size(),
                                                       self.metrics.get_value()))

    def spits_committer_commit_pit(self, user_data, result):
        # Create the pointer to result and result size
        cres, cressz = self.to_c_array(result)
        # Get the native pointer to the user data
        p_user_data = user_data.get_value()
        return self.module.spits_committer_commit_pit(p_user_data, cres, cressz)

    def spits_committer_commit_job(self, user_data, jobctx):
        fres = [None, None, None]
        # Get the native pointer to the user data
        p_user_data = user_data.get_value()

        # Create an inner converter for the callback
        def push(cfres, cfressz, ctx):
            # Thanks to python closures, the context is not
            # necessary, in any case, check for correctness
            fres[1] = (self.to_py_array(cfres, cfressz),)
            fres[2] = ctx

        # Commit job and get the final result
        fres[0] = self.module.spits_committer_commit_job(p_user_data,
                                                         self.cpusher(push), jobctx)
        # Return the pushed data along with the return code of the method
        return fres

    def spits_committer_finalize(self, user_data):
        # Get the native pointer to the user data
        p_user_data = user_data.get_value()
        # Optional function
        if not hasattr(self.module, 'spits_committer_finalize'):
            return
        # Is expected that the framework will not mess with the
        # value inside user_data so its ctype will remain unchanged
        return self.module.spits_committer_finalize(p_user_data)


class buffer_info(ctypes.Structure):
    _fields_ = [
        ('name', ctypes.c_char_p),
        ('type', ctypes.c_int),
        ('capacity', ctypes.c_uint)
    ]


class buffer_infos(ctypes.Structure):
    _fields_ = [
        ('quantity', ctypes.c_uint),
        ('infos', ctypes.POINTER(buffer_info))
    ]


class metric_union(ctypes.Union):
    _fields_ = [
        ('c_data', ctypes.POINTER(ctypes.c_ubyte)),
        ('i_data', ctypes.c_int),
        ('f_data', ctypes.c_float)
    ]


class metric_info(ctypes.Structure):
    _fields_ = [
        ('data', metric_union),
        ('data_size', ctypes.c_uint),
        ('seconds', ctypes.c_ulong),
        ('nano_seconds', ctypes.c_ulong),
        ('sequence_no', ctypes.c_uint),
    ]


class buffer_metrics(ctypes.Structure):
    _fields_ = [
        ('name', ctypes.c_char_p),
        ('type', ctypes.c_uint),
        ('num_metrics', ctypes.c_uint),
        ('metrics', ctypes.POINTER(metric_info))
    ]


element_types = {
    0: 'int',
    1: 'float',
    2: 'string',
    3: 'bytes'
}


class MetricManager:
    def __init__(self, job_binary: JobBinary, buffer_size: int = 10):
        self.job_binary = job_binary
        self.buffer_size = buffer_size

        self.job_binary.try_init_method('spits_metrics_new',
            ctypes.c_void_p, [ctypes.c_uint])
        self.job_binary.try_init_method('spits_metrics_delete',
            None, [ctypes.c_void_p])
        self.job_binary.try_init_method('spits_get_num_buffers',
            ctypes.c_uint, [ctypes.c_void_p])
        self.job_binary.try_init_method('spits_set_metric_int',
            None, [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int])
        self.job_binary.try_init_method('spits_set_metric_float',
            None, [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_float])
        self.job_binary.try_init_method('spits_set_metric_string',
            None, [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p])
        self.job_binary.try_init_method('spits_set_metric_bytes',
            None, [ctypes.c_void_p, ctypes.c_char_p,
                ctypes.POINTER(ctypes.c_byte), ctypes.c_uint
            ])
        self.job_binary.try_init_method('spits_get_metrics_list',
            ctypes.POINTER(buffer_infos), [ctypes.c_void_p])
        self.job_binary.try_init_method('spits_free_metric_list',
            None, [ctypes.POINTER(buffer_infos)])
        self.job_binary.try_init_method('spits_get_metrics_history',
            ctypes.POINTER(buffer_metrics),
            [ctypes.c_void_p, ctypes.c_uint, ctypes.POINTER(ctypes.c_char_p),
            ctypes.POINTER(ctypes.c_uint)])
        self.job_binary.try_init_method('spits_free_metrics_history',
            None, [ctypes.POINTER(buffer_metrics), ctypes.c_uint])

        self.metric_manager = self.job_binary.module.spits_metrics_new(buffer_size)

    def __del__(self):
        self.job_binary.module.spits_metrics_delete(self.metric_manager)

    def set_metric(self, metric_name: str, value: Union[int, float, str, bytes]):
        name = metric_name.encode('ascii')
        if type(value) is int:
            self.job_binary.module.spits_set_metric_int(
                self.metric_manager, name, value)
        elif type(value) is float:
            self.job_binary.module.spits_set_metric_float(
                self.metric_manager, name, value)
        elif type(value) is str:
            self.job_binary.module.spits_set_metric_string(
                self.metric_manager, name, value.encode('ascii'))
        elif type(value) is bytes:
            val = ctypes.cast(value, ctypes.POINTER(ctypes.c_byte))
            self.job_binary.module.spits_set_metric_bytes(
                self.metric_manager, name, val, len(value))
        else:
            raise TypeError(f"Invalid type {type(value)}")

    def get_metrics(self) -> dict:
        buffers = self.job_binary.module.spits_get_metrics_list(self.metric_manager)
        metrics = {}
        info = buffer_infos.from_address(ctypes.addressof(buffers[0]))
        for i in range(info.quantity):
            buffer_name = str(info.infos[i].name.decode('ascii'))
            buffer_type = info.infos[i].type
            buffer_capacity = info.infos[i].capacity
            metrics[buffer_name] = {
                'type': element_types[buffer_type],
                'capacity': buffer_capacity,
                'values': []
            }

        metric_names = list(metrics.keys())
        num_metrics = len(metric_names)
        names = (ctypes.c_char_p * num_metrics)(*[m.encode('ascii') for m in metric_names])
        sizes = (ctypes.c_uint * num_metrics)(*[ metrics[m]['capacity'] for m in metric_names ])
        metric_values = self.job_binary.module.spits_get_metrics_history(
            self.metric_manager, ctypes.c_uint(num_metrics), names, sizes
        )

        for i in range(num_metrics):
            metric = buffer_metrics.from_address(ctypes.addressof(metric_values[i]))
            m_name = metric_names[i]
            for j in range(metric.num_metrics):
                dsize = metric.metrics[j].data_size
                sec = metric.metrics[j].seconds
                nsec = metric.metrics[j].nano_seconds
                seq = metric.metrics[j].sequence_no
                if metrics[m_name]['type'] == 'int':
                    value = metric.metrics[j].data.i_data
                elif metrics[m_name]['type'] == 'float':
                    value = metric.metrics[j].data.f_data
                elif metrics[m_name]['type'] == 'string':
                    value = ctypes.cast(metric.metrics[j].data.c_data, ctypes.c_char_p)
                    value = str(value.value.decode('ascii'))
                else:
                    value = bytearray(dsize)
                    for k in range(dsize):
                        value[k] = metric.metrics[j].data.c_data[k]
                    value = base64.b64encode(value).decode('ascii')

                metrics[m_name]['values'].append({
                    'value': value,
                    'seconds': sec,
                    'nano_seconds': nsec,
                    'sequence_no': seq,
                    'data_size': dsize
                })

        self.job_binary.module.spits_free_metric_list(buffers)
        self.job_binary.module.spits_free_metrics_history(metric_values, num_metrics)
        return metrics