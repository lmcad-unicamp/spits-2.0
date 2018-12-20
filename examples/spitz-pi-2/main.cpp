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
#include <sstream>
#include <string>
#include <limits>
#include <vector>
#include <cmath>

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

// This class coordinates the execution of jobs
class spitz_main : public spitz::spitz_main
{
public:
    int main(int argc, const char* argv[], const spitz::runner& runner)
    {
        parameters p(argc, argv, "[SM] ");

        spitz::istream final_result;
        double pi_1, pi_2, pi_err;
        int r;

        // Let's do two steps: Compute pi with the provided number of
        // iteractions and then halving the number of iteractions

        std::stringstream ss;
        std::string n1;
        std::string n2;

        ss << p.n;
        n1 = ss.str();

        ss.str("");
        ss << p.n / 2;
        n2 = ss.str();

        const char* argv1[] = { argv[0], n1.c_str() };
        const char* argv2[] = { argv[0], n2.c_str() };

        // Run the first job and check the return
        std::cout << "[SM] Executing first job..." << std::endl;

        r = runner.run(2, argv1, final_result);
        if (r != 0) {
            std::cerr << "[SM] The execution of the first job "
                "failed with code " << r << "!" << std::endl;
            exit(1);
        }

        // We know the passed data is a double,
        // collect the first value of pi
        final_result >> pi_1;

        // Run the second job and check the return
        std::cout << "[SM] Executing second job..." << std::endl;

        r = runner.run(2, argv2, final_result);
        if (r != 0) {
            std::cerr << "[SM] The execution of the second job "
                "failed with code " << r << "!" << std::endl;
            exit(1);
        }

        // We know the passed data is a double,
        // collect the second value of pi
        final_result >> pi_2;

        // Calculate the error and print the output
        pi_err = std::abs(pi_2 - pi_1);

        std::cout.precision(17);
        std::cout << "[SM] First pi value is: " << std::fixed
            << pi_1 << std::endl
            << "[SM] Second pi value is: " << std::fixed
            << pi_2 << std::endl
            << "[SM] The error is: " << std::fixed
            << pi_err << std::endl;

        return 0;
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

        std::cout << "[JM] Generated task for i = " << i << "." << std::endl;

        // Increment the step by the amount to be packed for each worker
        i += p.split;

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
        // Our result stream
        spitz::ostream o;

        // Multiply the sum of terms by the step to get the
        // final result of pi and print it with the maximum
        // precision possible
        pival = pival * p.step;

        // Push the result
        o << pival;
        final_result.push(o);
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
    spitz::spitz_main *create_spitz_main()
    {
        return new spitz_main();
    }

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
