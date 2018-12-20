/*
 * The MIT License (MIT)
 *
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
#define SPITZ_ENTRY_POINT

// Spitz serial debug enables a Spitz-compliant main function to allow
// the module to run as an executable for testing purposes.
// #define SPITZ_SERIAL_DEBUG

#include <spitz/spitz.hpp>

#include <iostream>
#include <string>
#include <limits>

// Parameters should not be stored inside global variables because the
// Spitz interface does not guarantee memory isolation between job
// manager, committer and workers. This can lead to a race condition when
// each thread tries to initialize the parameters from argc and argv.
struct parameters
{
    std::string who;

    int n;
    double step;

    int split;

    parameters(int argc, const char *argv[], const std::string& who = "") :
        split(1000), who(who)
    {
        if (argc < 2 || argc > 3) {
            std::cerr << "USAGE: " << argv[0] << " N" << std::endl;
            exit(1);
        }

        n = atoi(argv[1]);
        step = 1.0 / (double)n;
    }

    void print()
    {
        std::cout
            << who << "N: " << n << std::endl
            << who << "step: " << step << std::endl;
    }
};

// This class creates tasks.
class job_manager : public spitz::job_manager
{
private:
    parameters p;
    int i;

public:
    job_manager(int argc, const char *argv[], spitz::istream& jobinfo) :
        p(argc, argv, "[JM] "), i(0)
    {
        p.print();

        std::cout << "[JM] Job manager created." << std::endl;
    }

    bool next_task(const spitz::pusher& task)
    {
        spitz::ostream o;

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
class worker : public spitz::worker
{
private:
    parameters p;

public:
    worker(int argc, const char *argv[]) : p(argc, argv, "[WK] ")
    {
        p.print();
        std::cout << "[WK] Worker created." << std::endl;
    }

    int run(spitz::istream& task, const spitz::pusher& result)
    {
        // Data stream used to store the output
        spitz::ostream o;

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

        // Send the result to the Spitz runtime
        result.push(o);

        std::cout << "[WK] Processed batch " << batch << "." << std::endl;
        return 0;
    }
};

// This class is responsible for merging the result of each individual task
// and, if necessary, to produce the final result after all of the task
// results have been received.
class committer : public spitz::committer
{
private:
    parameters p;
    double pival;

public:
    committer(int argc, const char *argv[], spitz::istream& jobinfo) :
        p(argc, argv, "[CO] "), pival(0.0)
    {
        p.print();
        std::cout << "[CO] Committer created." << std::endl;
    }

    int commit_task(spitz::istream& result)
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

    int commit_job(const spitz::pusher& final_result)
    {
        // Multiply the sum of terms by the step to get the
        // final result of pi and print it with the maximum
        // precision possible
        pival = pival * p.step;
        std::cout.precision(17);
        std::cout << "[CO] The value of pi is: " << std::fixed
            << pival << std::endl;

        // A result must be pushed even if it is an empty one
        final_result.push(NULL, 0);
        return 0;
    }

    ~committer()
    {

    }
};

// The factory binds the user code with the Spitz C++ wrapper code.
class factory : public spitz::factory
{
public:
    spitz::job_manager *create_job_manager(int argc, const char *argv[],
        spitz::istream& jobinfo)
    {
        return new job_manager(argc, argv, jobinfo);
    }

    spitz::worker *create_worker(int argc, const char *argv[])
    {
        return new worker(argc, argv);
    }

    spitz::committer *create_committer(int argc, const char *argv[],
        spitz::istream& jobinfo)
    {
        return new committer(argc, argv, jobinfo);
    }
};

// Creates a factory class.
spitz::factory *spitz_factory = new factory();
