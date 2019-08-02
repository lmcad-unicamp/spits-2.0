#!/usr/bin/env python

# The MIT License (MIT)
#
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

import ctypes, os, struct, sys

from libspitz import Blob, Pointer

# TODO try-except around C calls

class JobBinary(object):
    """Class binding for the external job binary loaded by spitz"""

    # Constructor
    def __init__(self, filename):
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
            ctypes.c_longlong
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
            ctypes.POINTER(ctypes.c_char_p)
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
            ctypes.c_longlong
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

        #Metrics

        self.try_init_method('spits_metric_new', ctypes.c_void_p, [
            ctypes.c_int
        ])

        self.try_init_method('spits_metric_finish', None, None)

        self.try_init_method('spits_get_metrics_list', ctypes.c_void_p, None)

        self.try_init_method('spits_get_metrics_last_values', ctypes.c_void_p, [
            ctypes.POINTER(ctypes.c_char_p)
        ])

        self.try_init_method('spits_get_metrics_history', ctypes.c_void_p, [
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.POINTER(ctypes.c_int)
        ])

        self.try_init_method('spits_set_metric_int', None, [
            ctypes.c_char_p,
            ctypes.c_int,
        ])


    def try_init_method(self, name, restype, argtypes):
        """Set the specified format for a method in the module,
        if it exists. Return true if it does exist."""
        if not hasattr(self.module, name):
            return False
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
        return Pointer(self.module.spits_job_manager_new(cargc,
            cargv, jobinfo.get_pointer(), jobinfo.get_size()))

    def spits_job_manager_next_task(self, user_data: Pointer, jmctx: int or Pointer) -> tuple:
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

    def spits_job_manager_finalize(self, user_data):
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
        return Pointer(self.module.spits_worker_new(cargc, cargv))

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
            jobinfo.get_pointer(), jobinfo.get_size()))

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


    def spits_metric_new(self, buffer_size):
        if not hasattr(self.module, 'spits_metric_new'):
            return
        return Pointer(self.module.spits_metric_new(ctypes.c_int(buffer_size)))

    def spits_metric_finish(self):
        if not hasattr(self.module, 'spitz_metric_finish'):
            return
        return self.module.spitz_metric_finish()


    def spits_get_metrics_list(self):
        if not hasattr(self.module, 'spits_get_metrics_list'):
            return

        metrics = []
        metrics_str = ctypes.cast(self.module.spits_get_metrics_list(), ctypes.POINTER(ctypes.c_ubyte))
        num_metrics = self.read_uint64(metrics_str, 0)
        cursor = 8

        for i in range(0, num_metrics):
            metric_name = self.read_string(metrics_str, cursor)
            cursor += len(metric_name)
            metric_size = self.read_uint64(metrics_str, cursor)
            cursor += 8
            metric_type = self.read_string(metrics_str, cursor)
            cursor += len(metric_type)
            metrics.append((metric_name, metric_size, metric_type))

        return metrics

    def spits_get_metrics_last_values(self, metric_names):
        argv = [x.encode('utf8') for x in metric_names]
        argv.append(None)
        cargv = (ctypes.c_char_p * len(argv))()
        cargv[:] = argv
        values = ctypes.cast(self.module.spits_get_metrics_last_values(cargv), ctypes.POINTER(ctypes.c_ubyte))

        cursor = 0
        last_values = []

        for i in range(0, len(metric_names)):
            metric_name = metric_names[i]
            #metric_name = self.read_string(values, cursor)
            #cursor += len(metric_name)
            value = self.read_uint64(values, cursor)
            cursor += 8
            tv_sec = self.read_uint64(values, cursor)
            cursor += 8
            tv_nsec = self.read_uint64(values, cursor)
            cursor += 8
            sequence_no = self.read_uint64(values, cursor)
            cursor += 8

            print("Metric: '" + str(metric_name) + "', last value: " + str(value) + ". Time: " + str(tv_sec) + ":" + str(
                tv_nsec) + " and sequence_no: " + str(sequence_no))
            last_values.append((str(metric_name), value, tv_sec, tv_nsec, sequence_no))

        return last_values


    def spits_set_metric_int(self, metric_name, value):  # value as int
        name = ctypes.c_char_p(metric_name.encode('utf-8'))
        value_int = ctypes.c_int(value)
        if not hasattr(self.module, 'spits_set_metric_int'):
            return
        return self.module.spits_set_metric_int(name, value_int)


    def read_string(self, values, initial):
        cursor_initial = initial
        cursor_final = initial

        while values[cursor_final] != 0x0:
            cursor_final += 1

        cursor_final += 1

        converted_str = ''.join(chr(i) for i in values[cursor_initial:cursor_final])
        return converted_str

    def read_uint64(self, values, initial):
        return int.from_bytes(bytearray(values[initial:initial + 8]), byteorder='big')


#        self.try_init_method('spits_get_metrics_list', ctypes.c_void_p, [
#            ctypes.c_void_p
#        ])
#
#        self.try_init_method('spits_get_metrics_last_values', ctypes.c_void_p, [
#            ctypes.c_void_p,
#            ctypes.POINTER(ctypes.c_char_p)
#        ])
#
#        self.try_init_method('spits_get_metrics_history', ctypes.c_void_p, [
#            ctypes.c_void_p,
#            ctypes.POINTER(ctypes.c_char_p),
#            ctypes.POINTER(ctypes.c_int)
#        ])
