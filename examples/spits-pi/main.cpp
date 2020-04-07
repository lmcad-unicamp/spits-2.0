/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Edson Borin <edson@ic.unicamp.br>
 * Copyright (c) 2016 Caian Benedicto <caian@ggaunicamp.com>
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


// This define enables the C++ wrapping code around the C API, it must be used
// only once per C++ module.
#define SPITS_ENTRY_POINT

// Spits serial debug enables a Spits-compliant main function to allow
// the module to run as an executable for testing purposes.
// #define SPITS_SERIAL_DEBUG

#include <spits.hpp>
#include <iostream>
#include <string>
#include <limits>
#include <math.h>       /* ceil */

// Parameters should not be stored inside global variables because the
// Spits interface does not guarantee memory isolation between job
// manager, committer and workers. This can lead to a race condition when
// each thread tries to initialize the parameters from argc and argv.
struct parameters

{
    std::string who;

    int n;
    double step;

    int split;

    parameters(int argc, const char *argv[], const std::string& who = "") :
      who(who)
    {
        if (argc < 3 || argc > 3) {
	  std::cerr << "USAGE: " << argv[0] << " NSAMPLES SAMPLES_PER_TASK" << std::endl
		    << std::endl
		    << " This program computes the value of PI using the Monte Carlo method." << std::endl
		    << std::endl
		    << " NSAMPLES        : the number of samples computed by the method." << std::endl
		    << " SAMPLES_PER_TASK: the number of samples computed by each SPITS task." << std::endl
		    << std::endl
		    << " PI accuracy: You may increase the number of samples (NSAMPLES) to obtain" << std::endl
		    << " a more accurate value for pi." << std::endl
		    << std::endl
		    << " A note on performance: Due to communication and data management overheads," << std::endl
		    << " SPITS jobs work well with the PYPITS runtime when the amount of work " << std::endl
		    << " performed by each task is not too small." << std::endl
		    << " If you want to increase the amount of work performed by each task, just" << std::endl
		    << " increase the value of SAMPLES_PER_TASK. " << std::endl
		    << " Increasing the value of SAMPLES_PER_TASK will also reduce the amount of" << std::endl
		    << " tasks to be processed. Notice that, if this ammount is too small, a system" << std::endl
		    << " with multiple SPITS workers may starve due to lack of tasks. " << std::endl;
	  exit(1);
        }

        n     = atoi(argv[1]);
        split = atoi(argv[2]);
        step = 1.0 / (double)n;
    }

    void print()
    {
        std::cout
            << who << "NSAMPLES: " << n << std::endl
            << who << "SAMPLES_PER_TASK: " << split << std::endl
            << who << "Total number of tasks:" << ceil((double)n/(double)split) << std::endl;
 
  }
};

// This class creates tasks.
class job_manager : public spits::job_manager
{
private:
    parameters p;
    int i;

public:
    job_manager(int argc, const char *argv[], spits::istream& jobinfo) :
        p(argc, argv, "[JM] "), i(0)
    {
        p.print();

        std::cout << "[JM] Job manager created." << std::endl;
    }

    bool next_task(const spits::pusher& task)
    {
        spits::ostream o;

        if (i >= p.n)
            return false;

        // Push the current step
        o << i;

        // Increment the step by the amount to be packed for each worker
        i += p.split;

        std::cout << "[JM] Generated task for i = " << i << "." << std::endl;

        // Serialize and push the task
        task.push(o);
        return true;
    }

    ~job_manager()
    {
    }
};

// This class executes tasks, preferably without modifying its internal state
// because this can lead to a break of idempotence between tasks. The 'run'
// method will not impose a 'const' behavior to allow libraries that rely
// on changing its internal state, for instance, OpenCL (see clpi example).
class worker : public spits::worker
{
private:
    parameters p;

public:
    worker(int argc, const char *argv[]) : p(argc, argv, "[WK] ")
    {
        p.print();
        std::cout << "[WK] Worker created." << std::endl;
    }

    int run(spits::istream& task, const spits::pusher& result)
    {
        // Data stream used to store the output
        spits::ostream o;

        // Read the current step from the task
        int i, batch;
        task >> i;
        batch = i;

        // Get the number of steps to compute
        int n = std::min<int>(p.n, i + p.split);

        // Compute each term of the pi expansion
        for ( ; i < n; i++) {
            double x = (static_cast<double>(i) + 0.5) * p.step;
            x = 4.0 / (1.0 + x * x);
            // Add the term to the data stream
            o << x;
        }

        // Send the result to the Spits runtime
        result.push(o);

        std::cout << "[WK] Processed batch " << batch << "." << std::endl;
        return 0;
    }
};

// This class is responsible for merging the result of each individual task
// and, if necessary, to produce the final result after all of the task
// results have been received.
class committer : public spits::committer
{
private:
    parameters p;
    double pival;

public:
    committer(int argc, const char *argv[], spits::istream& jobinfo) :
        p(argc, argv, "[CO] "), pival(0.0)
    {
        p.print();
        std::cout << "[CO] Committer created." << std::endl;
    }

    int commit_task(spits::istream& result)
    {
        std::cout << "[CO] Committing result..." << std::endl;

        // Accumulate each term of the expansion
        while(result.has_data()) {
            double d;
            result >> d;
            pival += d;
        }

        return 0;
    }

    int commit_job(const spits::pusher& final_result)
    {
        // Multiply the sum of terms by the step to get the
        // final result of pi and print it with the maximum
        // precision possible
        pival = pival * p.step;
	double correct_pi = 3.141592653589793238;
        std::cout.precision(17);
        std::cout << "[CO] The real value of pi with 18 decimal places is: " << std::fixed
		  << correct_pi << std::endl;
        std::cout << "[CO] The value by the monte carlo method is: " << std::fixed
		  << pival << std::endl;
        std::cout << "[CO] The error is approximately: " << std::fixed
		  << (double) correct_pi - (double) pival << std::endl;

        // A result must be pushed even if it is an empty one
        final_result.push(NULL, 0);
        return 0;
    }

    ~committer()
    {

    }
};

// The factory binds the user code with the Spits C++ wrapper code.
class factory : public spits::factory
{
public:
    spits::job_manager *create_job_manager(int argc, const char *argv[],
        spits::istream& jobinfo, spits::metrics& metrics)
    {
        metrics.add_metric("JobManager created", "jobpi");
        return new job_manager(argc, argv, jobinfo);
    }

    spits::worker *create_worker(int argc, const char *argv[], spits::metrics& metrics)
    {
        metrics.add_metric("Worker created", "jobpi");
        return new worker(argc, argv);
    }

    spits::committer *create_committer(int argc, const char *argv[],
        spits::istream& jobinfo, spits::metrics& metrics)
    {
        metrics.add_metric("Commiter created", "jobpi");
        return new committer(argc, argv, jobinfo);
    }
};

// Creates a factory class.
spits::factory *spits_factory = new factory();
