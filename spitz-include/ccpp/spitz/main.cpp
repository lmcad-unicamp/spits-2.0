#include <iostream>
#include <vector>
#include <cstring>
#include <ctime>
#include <exception>
#include "metrics.hpp"

/*#define ASSERT(a) \
    do{ \
        if(!a) throw std::runtime_error("" __FUNC__ ":"  __LINE__ " asserted to false."); \
    } while(0)
#define EXPECT_FALSE(a) (ASSERT((a) == (0)))
#define EXPECT_TRUE(a) (ASSERT((a) != (0)))
#define EXPECT_GT(a, b) (ASSERT((a) > (b)))
#define EXPECT_LT(a, b) (ASSERT((a) < (b)))
#define EXPECT_GE(a, b) (ASSERT((a) >= (b)))
#define EXPECT_LE(a, b) (ASSERT((a) <= (b)))*/

/* Tests the 'Metrics' class */
bool test_1(void) {
    const int n = 100;
    std::vector<std::string> test_names = {"test_a", "test_b"};
    std::vector<int> tests_hist_sizes = {50, 51};
    //std::cout << __FUNC__ << ": Parameters\nn=" << n;
    spitz::Metrics metrics(n-1);
    struct timespec now;
    for (int64_t i=0; i<150; ++i) {
        int64_t j = i*2;
        metrics.set_metric(test_names[0].c_str(), (void*) &i);
        metrics.set_metric(test_names[1].c_str(), (void*) &j);
    }


    spitz::istream ss((char*) metrics.get_metrics_list(), 100);
    int64_t no_elements = ss.read_longlong();
    std::cout << "no-elements " << no_elements << "\n" <<  ss.read_string() << " " << ss.read_ulonglong() << " " << ss.read_string() << "\n" <<  ss.read_string() << " " << ss.read_ulonglong() << " " << ss.read_string() << "\n";

    std::cout << "----------------------------------\n";

    spitz::istream ss2((char*) metrics.get_metrics_last_values(test_names), 1000);
    for (int i =0; i<test_names.size(); ++i) {
        std::cout << ss2.read_longlong() << " " << ss2.read_longlong() << ":" << ss2.read_longlong() << ", seq: " << ss2.read_longlong() << "\n";
    }

    std::cout << "----------------------------------\n";

    spitz::istream ss3((char*) metrics.get_metrics_history(test_names, tests_hist_sizes), 10000);
    for (int i=0; i<test_names.size(); ++i) {
        int64_t initial_seq_no = ss3.read_longlong();
        int64_t actual_size = ss3.read_longlong();
        std::cout << "Initial seq_no: " << initial_seq_no << " and size: " << actual_size << "\n";
        for (int j=0; j<actual_size; ++j) {
            std::cout << ss3.read_longlong() << " " << ss3.read_longlong() << ":" << ss3.read_longlong() << "\t";
        }
        std::cout << "\n";
    }

    std::cout << "----------------------------------\n";
    return true;
}

extern "C" bool test_2(void) {
    const int n = 30;
    std::vector<std::string> test_names = {"test_a", "test_b"};
    std::vector<int> tests_hist_sizes = {50, 51};
    spitz::Metrics* metrics = reinterpret_cast<spitz::Metrics*>(spitz_metric_new(n));

    for (int64_t i=0; i<150; ++i) {
        int64_t j = i*2;
        spits_set_metric((void*) metrics, (char*)test_names[0].c_str(), (void*) &i);
        spits_set_metric((void*) metrics, (char*)test_names[1].c_str(), (void*) &j);
        //metrics.set_metric(test_names[1].c_str(), (void*) &j);
    }


    spitz::istream ss((char*) spits_get_metrics_list((void*) metrics), 100);
    int64_t no_elements = ss.read_longlong();
    std::cout << "no-elements " << no_elements << "\n" <<  ss.read_string() << " " << ss.read_ulonglong() << " " << ss.read_string() << "\n" <<  ss.read_string() << " " << ss.read_ulonglong() << " " << ss.read_string() << "\n";

    std::cout << "----------------------------------\n";

    char** list = new char*[test_names.size()+1];
    for (int i =0; i<test_names.size(); ++i)
        list[i] = (char*)test_names[i].c_str();
    list[test_names.size()] = nullptr;

    spitz::istream ss2((char*) spits_get_metrics_last_values((void*) metrics, (const char**) list), 1000);
    for (int i =0; i<test_names.size(); ++i) {
        std::cout << ss2.read_longlong() << " " << ss2.read_longlong() << ":" << ss2.read_longlong() << ", seq: " << ss2.read_longlong() << "\n";
    }

    std::cout << "----------------------------------\n";

    int *n_list = new int[2] {50, 52};
    spitz::istream ss3((char*) spits_get_metrics_history((void*) metrics, (const char**) list, n_list), 10000);
    for (int i=0; i<test_names.size(); ++i) {
        int64_t initial_seq_no = ss3.read_longlong();
        int64_t actual_size = ss3.read_longlong();
        std::cout << "Initial seq_no: " << initial_seq_no << " and size: " << actual_size << "\n";
        for (int j=0; j<actual_size; ++j) {
            std::cout << ss3.read_longlong() << " " << ss3.read_longlong() << ":" << ss3.read_longlong() << "\t";
        }
        std::cout << "\n";
    }

    std::cout << "----------------------------------\n";

    delete[] list;
    delete[] n_list;
    spitz_metric_finish((void*) metrics);
}

int main(int argc, const char * argv[])
{
    //std::cout << "Running Test 1" << std::endl;
    //test_1();

    std::cout << "Running Test 2" << std::endl;
    test_2();
    //EXPECT_TRUE(test_1());
    /*RingBuffer<int> values(10);

    for (int i=0; i<20; ++i) {
        //int* a = new int(i*i);
        values.push(i*i);
    }

    while(!values.empty())
        std::cout << "Popped value " << (const int) values.pop() << std::endl;

    return 0;*/
}
