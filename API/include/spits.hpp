/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Otávio Napoli <otavio.napoli@gmail.com>
 * Copyright (c) 2020 Edson Borin <edson@ic.unicamp.br>
 * Copyright (c) 2015 Caian Benedicto <caian@ggaunicamp.com>
 * Copyright (c) 2014 Ian Liu Rodrigues <ian.liu@ggaunicamp.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#ifndef __SPITS_CPP__
#define __SPITS_CPP__

#include "spits.h"
#include "stream.hpp"
#include "metrics.hpp"
#include <iostream>

namespace spits
{

    /**
     * Push information (data/ostream) to a TM/JM using a callback function
     *
     * @param pushf JM/TM Callback function to be called when data is added
     * @param ctx   Default task context (task id)
     */
    class pusher
    {
    private:
        spitspush_t pushf;
        spitsctx_t ctx;

    public:
        pusher(spitspush_t pushf, spitsctx_t ctx) :
            pushf(pushf), ctx(ctx)
        {
        }

        /**
         * Push data to JM/TM via the defined callback
         * @param p Output stream data
         */
        void push(const ostream& p) const
        {
            this->pushf(p.data(), p.pos(), this->ctx);
        }

        /**
         * Push data to JM/TM via the defined callback
         * @param p Data
         * @param s Size
         */
        void push(const void* p, spitssize_t s) const
        {
            this->pushf(p, s, this->ctx);
        }
    };

    class runner
    {
    private:
        spitsrun_t runf;

    public:
        runner(spitsrun_t runf) : runf(runf)
        {
        }

        int run(int argc, const char** argv) const
        {
            ostream j;
            istream r;
            return run(argc, argv, j, r);
        }

        int run(int argc, const char** argv, istream& final_result) const
        {
            ostream j;
            return run(argc, argv, j, final_result);
        }

        int run(int argc, const char** argv, ostream& jobinfo) const
        {
            istream r;
            return run(argc, argv, jobinfo, r);
        }

        int run(int argc, const char** argv, ostream& jobinfo,
            istream& final_result) const
        {
            const void* pfinal_result;
            spitssize_t pfinal_result_size;
            const void* pjobinfo = jobinfo.data();
            spitssize_t pjobinfo_size = jobinfo.pos();
            int r = this->runf(argc, argv, pjobinfo, pjobinfo_size,
                &pfinal_result, &pfinal_result_size);
            final_result = istream(pfinal_result, pfinal_result_size);
            return r;
        }
    };

    class spits_main
    {
    public:
        virtual int main(int argc, const char* argv[], const runner& runner)
        {
            return runner.run(argc, argv);
        }
        virtual ~spits_main(){ }
    };

    class job_manager
    {
    public:
        virtual bool next_task(const pusher& task) = 0;
        virtual ~job_manager() { }
    };

    class worker
    {
    public:
        virtual int run(istream& task, const pusher& result) = 0;
        virtual ~worker() { }
    };

    class committer
    {
    public:
        virtual int commit_task(istream& result) = 0;
        virtual int commit_job(const pusher& final_result) {
            final_result.push(NULL, 0);
            return 0;
        }
        virtual ~committer() { }
    };

    class factory
    {
    public:
        virtual spits_main *create_spits_main() { return new spits_main(); }
        virtual spits::metrics *create_metrics(unsigned int size) { return new spits::metrics(size); }
        virtual job_manager *create_job_manager(int, const char *[], istream&, spits::metrics&) = 0;
        virtual worker *create_worker(int, const char *[], spits::metrics&) = 0;
        virtual committer *create_committer(int, const char *[], istream&, spits::metrics&) = 0;
    };
};

extern spits::factory *spits_factory;

#ifdef SPITS_ENTRY_POINT

extern "C" int spits_main(int argc, const char* argv[], spitsrun_t run)
{
    spits::spits_main *sm = spits_factory->create_spits_main();
    spits::runner runner(run);

    int r = sm->main(argc, argv, runner);

    delete sm;

    return r;
}

extern "C" void *spits_job_manager_new(int argc, const char *argv[],
    const void* jobinfo, spitssize_t jobinfosz, const void* metrics)
{
    spits::istream ji(jobinfo, jobinfosz);
    spits::metrics* m = const_cast<spits::metrics*>(reinterpret_cast<const spits::metrics *>(metrics));
    spits::job_manager *jm = spits_factory->create_job_manager(argc,
        argv, ji, *m);
    return reinterpret_cast<void*>(jm);
}

extern "C" int spits_job_manager_next_task(void *user_data,
    spitspush_t push_task, spitsctx_t jmctx)
{
    class spits::job_manager *jm = reinterpret_cast
        <spits::job_manager*>(user_data);

    spits::pusher task(push_task, jmctx);
    return jm->next_task(task) ? 1 : 0;
}

extern "C" void spits_job_manager_finalize(void *user_data)
{
    class spits::job_manager *jm = reinterpret_cast
        <spits::job_manager*>(user_data);

    delete jm;
}

extern "C" void *spits_worker_new(int argc, const char **argv, const void* metrics)
{
    spits::metrics* m = const_cast<spits::metrics*>(reinterpret_cast<const spits::metrics *>(metrics));
    spits::worker *w = spits_factory->create_worker(argc, argv, *m);
    return reinterpret_cast<void*>(w);
}

extern "C" int spits_worker_run(void *user_data, const void* task,
    spitssize_t tasksz, spitspush_t push_result,
    spitsctx_t taskctx)
{
    spits::worker *w = reinterpret_cast
        <spits::worker*>(user_data);

    spits::istream stask(task, tasksz);
    spits::pusher result(push_result, taskctx);

    return w->run(stask, result);
}

extern "C" void spits_worker_finalize(void *user_data)
{
    spits::worker *w = reinterpret_cast
        <spits::worker*>(user_data);

    delete w;
}

extern "C" void *spits_committer_new(int argc, const char *argv[],
    const void* jobinfo, spitssize_t jobinfosz, const void* metrics)
{
    spits::istream ji(jobinfo, jobinfosz);
    spits::metrics* m = const_cast<spits::metrics*>(reinterpret_cast<const spits::metrics *>(metrics));
    spits::committer *co = spits_factory->create_committer(argc, argv, ji, *m);
    return reinterpret_cast<void*>(co);
}

extern "C" int spits_committer_commit_pit(void *user_data,
    const void* result, spitssize_t resultsz)
{
    spits::committer *co = reinterpret_cast
        <spits::committer*>(user_data);

    spits::istream sresult(result, resultsz);
    return co->commit_task(sresult);
}

extern "C" int spits_committer_commit_job(void *user_data,
    spitspush_t push_final_result, spitsctx_t jobctx)
{
    spits::committer *co = reinterpret_cast
        <spits::committer*>(user_data);

    spits::pusher final_result(push_final_result, jobctx);
    return co->commit_job(final_result);
}

extern "C" void spits_committer_finalize(void *user_data)
{
    spits::committer *co = reinterpret_cast
        <spits::committer*>(user_data);

    delete co;
}

#ifdef SPITS_SERIAL_DEBUG
#include <vector>
#include <algorithm>
#include <iostream>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <stdint.h>

static void spits_debug_pusher(const void* pdata,
    spitssize_t size, spitsctx_t ctx)
{
    std::vector<uint8_t>* v = reinterpret_cast<std::vector<uint8_t>*>
      (const_cast<void*>(ctx));

    if (v->size() != 0) {
        std::cerr << "[SPITS] Push called more than once!" << std::endl;
    }

    v->push_back(1);

    std::copy(reinterpret_cast<const uint8_t*>(pdata),
        reinterpret_cast<const uint8_t*>(pdata) + size,
        std::back_inserter(*v));
}

static int spits_debug_runner(int argc, const char** argv,
    const void* pjobinfo, spitssize_t jobinfosz,
    const void** pfinal_result, spitssize_t* pfinal_resultsz)
{
    void* metrics = spits_metrics_new(10);
    void* jm = spits_job_manager_new(argc, argv, pjobinfo, jobinfosz, metrics);
    void* co = spits_committer_new(argc, argv, pjobinfo, jobinfosz, metrics);
    void* wk = spits_worker_new(argc, argv, metrics);

    static int64_t jid = 0;
    int64_t tid = 0;

    int r1 = 0, r2 = 0, r3 = 0;

    std::vector<int8_t> task;
    std::vector<int8_t> result;
    std::vector<int8_t>* final_result = new std::vector<int8_t>();

    while(true) {
        task.clear();
        std::cerr << "[SPITS] Generating task " << tid << "..." << std::endl;
        if(!spits_job_manager_next_task(jm, spits_debug_pusher, &task))
            break;

        if (task.size() == 0) {
            std::cerr << "[SPITS] Task manager didn't push a task!"
                << std::endl;
            exit(1);
        }

        spits_set_metric_int(metrics, "Generated Tasks", tid);

        result.clear();
        std::cerr << "[SPITS] Executing task " << tid << "..." << std::endl;
        r1 = spits_worker_run(wk, task.data()+1, task.size()-1,
            spits_debug_pusher, &result);

        if (r1 != 0) {
            std::cerr << "[SPITS] Task " << tid << " failed to execute!"
                << std::endl;
            goto dump_task_and_exit;
        }


        if (task.size() == 0) {
            std::cerr << "[SPITS] Worker didn't push a result!"
                << std::endl;
            goto dump_task_and_exit;
        }

        spits_set_metric_int(metrics, "Executed Tasks", tid);

        std::cerr << "[SPITS] Committing task " << tid << "..." << std::endl;
        r2 = spits_committer_commit_pit(co, result.data()+1, result.size()-1);

        if (r2 != 0) {
            std::cerr << "[SPITS] Task " << tid << " failed to commit!"
                << std::endl;
            goto dump_result_and_exit;
        }

        spits_set_metric_int(metrics, "Commited Tasks", tid);
        tid++;
    }
    std::cerr << "[SPITS] Finished processing tasks." << std::endl;

    final_result->clear();
    std::cerr << "[SPITS] Committing job " << jid << "..." << std::endl;
    r3 = spits_committer_commit_job(co, spits_debug_pusher, final_result);

    if (r3 != 0) {
        std::cerr << "[SPITS] Job " << jid << " failed to commit!"
            << std::endl;
        exit(1);
    }

    if (final_result->size() <= 1) {
        *pfinal_result = NULL;
        *pfinal_resultsz = 0;
    } else {
        *pfinal_result = final_result->data()+1;
        *pfinal_resultsz = final_result->size()-1;
    }

    std::cerr << "[SPITS] Finalizing task manager..." << std::endl;
    spits_job_manager_finalize(jm);

    std::cerr << "[SPITS] Finalizing committer..." << std::endl;
    spits_committer_finalize(co);

    std::cerr << "[SPITS] Finalizing worker..." << std::endl;
    spits_worker_finalize(wk);

    spits_metrics_delete(metrics);

    std::cerr << "[SPITS] Job " << jid << " completed." << std::endl;
    jid++;

    delete final_result;

    return 0;

dump_result_and_exit:
    {
        std::cerr << "[SPITS] Generating result dump for task "
            << tid << "..." << std::endl;
        std::stringstream ss;
        ss << "result-" << tid << ".dump";
        std::ofstream resfile(ss.str().c_str(), std::ofstream::binary);
        resfile.write(reinterpret_cast<const char*>(result.data()+1),
            result.size()-1);
        resfile.close();
        std::cerr << "[SPITS] Result dump generated as " << ss.str() <<
            " [" << (result.size()-1) << " bytes]. " << std::endl;
    }
dump_task_and_exit:
    {
        std::cerr << "[SPITS] Generating task dump for task "
            << tid << "..." << std::endl;
        std::stringstream ss;
        ss << "task-" << tid << ".dump";
        std::ofstream taskfile(ss.str().c_str(), std::ofstream::binary);
        taskfile.write(reinterpret_cast<const char*>(task.data()+1),
            task.size()-1);
        taskfile.close();
        std::cerr << "[SPITS] Task dump generated as " << ss.str() <<
            " [" << (task.size()-1) << " bytes]. " << std::endl;
    }

    spits_metrics_delete(metrics);
    delete final_result;
    exit(1);
}

int main(int argc, const char** argv)
{
    std::cerr << "[SPITS] Entering debug mode..." << std::endl;
    spits_main(argc, argv, spits_debug_runner);
    std::cerr << "[SPITS] Spits finished." << std::endl;

    delete spits_factory;
    return 0;
}
#endif

#endif

#endif /* __SPITS_CPP__ */

// vim:ft=cpp:sw=2:et:sta
