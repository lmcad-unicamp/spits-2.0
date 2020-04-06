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
// bool test_1(void) {
//     const int n = 100;
//     std::vector<std::string> test_names = {"test_a", "test_b"};
//     std::vector<int> tests_hist_sizes = {50, 51};
//     //std::cout << __FUNC__ << ": Parameters\nn=" << n;
//     spits::Metrics metrics(n-1);
//     struct timespec now;
//     for (int64_t i=0; i<150; ++i) {
//         int64_t j = i*2;
//         metrics->set_metric(test_names[0].c_str(), (void*) &i);
//         metrics->set_metric(test_names[1].c_str(), (void*) &j);
//     }
//
//
//     spits::istream ss((char*) metrics->get_metrics_list(), 100);
//     int64_t no_elements = ss.read_longlong();
//     std::cout << "no-elements " << no_elements << "\n" <<  ss.read_string() << " " << ss.read_ulonglong() << " " << ss.read_string() << "\n" <<  ss.read_string() << " " << ss.read_ulonglong() << " " << ss.read_string() << "\n";
//
//     std::cout << "----------------------------------\n";
//
//     spits::istream ss2((char*) metrics->get_metrics_last_values(test_names), 1000);
//     for (int i =0; i<test_names.size(); ++i) {
//         std::cout << ss2.read_longlong() << " " << ss2.read_longlong() << ":" << ss2.read_longlong() << ", seq: " << ss2.read_longlong() << "\n";
//     }
//
//     std::cout << "----------------------------------\n";
//
//     spits::istream ss3((char*) metrics->get_metrics_history(test_names, tests_hist_sizes), 10000);
//     for (int i=0; i<test_names.size(); ++i) {
//         int64_t initial_seq_no = ss3.read_longlong();
//         int64_t actual_size = ss3.read_longlong();
//         std::cout << "Initial seq_no: " << initial_seq_no << " and size: " << actual_size << "\n";
//         for (int j=0; j<actual_size; ++j) {
//             std::cout << ss3.read_longlong() << " " << ss3.read_longlong() << ":" << ss3.read_longlong() << "\t";
//         }
//         std::cout << "\n";
//     }
//
//     std::cout << "----------------------------------\n";
//     return true;
// }
//
// extern "C" bool test_2(void) {
//     const int n = 30;
//     std::vector<std::string> test_names = {"test_a", "test_b"};
//     std::vector<int> tests_hist_sizes = {50, 51};
//     spits::Metrics* metrics = reinterpret_cast<spits::Metrics*>(spits_metric_new(n));
//
//     for (int64_t i=0; i<150; ++i) {
//         int64_t j = i*2;
//         spits_set_metric((void*) metrics, (char*)test_names[0].c_str(), (void*) &i);
//         spits_set_metric((void*) metrics, (char*)test_names[1].c_str(), (void*) &j);
//         //metrics->set_metric(test_names[1].c_str(), (void*) &j);
//     }
//
//
//     spits::istream ss((char*) spits_get_metrics_list((void*) metrics), 100);
//     int64_t no_elements = ss.read_longlong();
//     std::cout << "no-elements " << no_elements << "\n" <<  ss.read_string() << " " << ss.read_ulonglong() << " " << ss.read_string() << "\n" <<  ss.read_string() << " " << ss.read_ulonglong() << " " << ss.read_string() << "\n";
//
//     std::cout << "----------------------------------\n";
//
//     char** list = new char*[test_names.size()+1];
//     for (int i =0; i<test_names.size(); ++i)
//         list[i] = (char*)test_names[i].c_str();
//     list[test_names.size()] = nullptr;
//
//     spits::istream ss2((char*) spits_get_metrics_last_values((void*) metrics, (const char**) list), 1000);
//     for (int i =0; i<test_names.size(); ++i) {
//         std::cout << ss2.read_longlong() << " " << ss2.read_longlong() << ":" << ss2.read_longlong() << ", seq: " << ss2.read_longlong() << "\n";
//     }
//
//     std::cout << "----------------------------------\n";
//
//     int *n_list = new int[2] {50, 52};
//     spits::istream ss3((char*) spits_get_metrics_history((void*) metrics, (const char**) list, n_list), 10000);
//     for (int i=0; i<test_names.size(); ++i) {
//         int64_t initial_seq_no = ss3.read_longlong();
//         int64_t actual_size = ss3.read_longlong();
//         std::cout << "Initial seq_no: " << initial_seq_no << " and size: " << actual_size << "\n";
//         for (int j=0; j<actual_size; ++j) {
//             std::cout << ss3.read_longlong() << " " << ss3.read_longlong() << ":" << ss3.read_longlong() << "\t";
//         }
//         std::cout << "\n";
//     }
//
//     std::cout << "----------------------------------\n";
//
//     delete[] list;
//     delete[] n_list;
//     spits_metric_finish((void*) metrics);
// }
//

int main(int argc, char** argv)
{
    // IntBuffer ibuffer(3);
    // FloatBuffer fbuffer(3);
    // BytesBuffer bbuffer(3);
    //
    // int i = 5;
    //
    // //INT ADD
    // ibuffer.add_element(i);
    // ibuffer.add_element(30);
    // ibuffer.add_element(40);
    // ibuffer.add_element(50);
    //
    // //FLOAT ADD
    // fbuffer.add_element(40.5);
    // fbuffer.add_element(40.7);
    // fbuffer.add_element(2.1);
    //
    // //CHAR ADD
    // unsigned char* barray = new unsigned char[20];
    // std::memcpy(barray, "abcdefghi1234567890", 20);
    // bbuffer.add_element(barray);
    //
    // barray = new unsigned char[20];
    // std::memcpy(barray, "xxcdefghi12345678xx", 20);
    // bbuffer.add_element(barray);
    //
    // //Create Unique ELements
    // std::unique_ptr<IntElement> ielement = std::unique_ptr<IntElement>(ibuffer.get_front_element());
    // std::unique_ptr<FloatElement> felement(fbuffer.get_element());
    // std::unique_ptr<BytesElement> belement;
    //
    // //GET int values
    // ielement = std::unique_ptr<IntElement>(ibuffer.get_front_element());
    // ielement = std::unique_ptr<IntElement>(ibuffer.get_front_element());
    // if (ielement)
    //     std::cout << ielement->to_string() << std::endl;

    // ielement = std::unique_ptr<IntElement>(ibuffer.get_element());
    // ielement = std::unique_ptr<IntElement>(ibuffer.get_element());
    //
    // //GET float values
    // felement = std::unique_ptr<FloatElement>(fbuffer.get_element());
    //
    // //GET bytes values
    // belement = std::unique_ptr<BytesElement>(bbuffer.get_element());
    //
    // //Print if element exists!
    // std::cout << "----------- PRINT METRIC ELEMENTS 1 -----------" << std::endl;
    // if (ielement)
    //     std::cout << ielement->to_string() << std::endl;
    // if (felement)
    //     std::cout << felement->to_string() << std::endl;
    // if (belement)
    //     std::cout << belement->to_string() << std::endl;

    //spits_metrics_new(3);
    //INT ADD
    // metrics->add_metric("metric_1", 10);
    // metrics->add_metric("metric_1", 20);
    // metrics->add_metric("metric_1", 30);
    // metrics->add_metric("metric_1", 40);
    spits_set_metric_int("metric_1", 10);
    spits_set_metric_int("metric_1", 20);
    spits_set_metric_int("metric_1", 30);
    spits_set_metric_int("metric_1", 40);

    //FLOAT ADD
    // metrics->add_metric("metric_2", (float) 3298.213289);
    // metrics->add_metric("metric_2", (float) 982101.312);
    // metrics->add_metric("metric_2", (float) 982101e+10);
    spits_set_metric_float("metric_2", (float) 3298.213289);
    spits_set_metric_float("metric_2", (float) 982101.312);
    spits_set_metric_float("metric_2", (float) 982101e+10);

    //CHAR ADD
    unsigned char* barray = new unsigned char[20];
    //barray = new unsigned char[20];
    std::memcpy(barray, "abcdefghi1234567890", 20);
    spits_set_metric_bytes("metric_3", barray);
    barray = new unsigned char[20];
    std::memcpy(barray, "xxcdefghi12345678xx", 20);
    spits_set_metric_bytes("metric_3", barray);
    barray = new unsigned char[20];
    std::memcpy(barray, "kkkkefghi123456kkkk", 20);
    spits_set_metric_bytes("metric_3", barray);
    barray = new unsigned char[20];
    std::memcpy(barray, "!YOU ONLY LIVE ONCE!", 20);
    spits_set_metric_bytes("metric_3", barray);


    // std::cout << "----------- PRINT METRIC ELEMENTS 2 -----------" << std::endl;
    //
    // auto int_metric = spits_metrics->get_metric("metric_1");
    // auto data = std::get<0>(int_metric);
    // if(data)
    //     delete data;
    //
    // int_metric = spits_metrics->get_metric("metric_1");
    // data = std::get<0>(int_metric);
    //
    // if (std::get<1>(int_metric) == TYPE_INT) {
    //     IntElement* ie = reinterpret_cast<IntElement*>(data);
    //     if(ie)
    //         std::cout << ie->to_string() << std::endl;
    // }
    //
    // if(data)
    //     delete data;
    //
    //
    //
    // auto float_metric = spits_metrics->get_metric("metric_2");
    // auto fdata = std::get<0>(float_metric);
    //
    // if (std::get<1>(float_metric) == TYPE_FLOAT) {
    //     FloatElement* fe = reinterpret_cast<FloatElement*>(fdata);
    //     if(fe)
    //         std::cout << fe->to_string() << std::endl;
    // }
    //
    // if(fdata)
    //     delete fdata;
    //
    //
    //
    //
    //
    // auto bytes_metric = spits_metrics->get_metric("metric_3");
    // auto bdata = std::get<0>(bytes_metric);
    // if(bdata)
    //     delete bdata;
    //
    // bytes_metric = spits_metrics->get_metric("metric_3");
    // bdata = std::get<0>(bytes_metric);
    //
    // if (std::get<1>(bytes_metric) == TYPE_BYTES) {
    //     BytesElement* be = reinterpret_cast<BytesElement*>(bdata);
    //     if(be)
    //         std::cout << be->to_string() << std::endl;
    // }
    //
    // if(bdata)
    //     delete bdata;

    void* x = spits_get_metrics_list();
    spits_free_ptr(x);

    //void* y = spits_get_metrics_last_values((const char**) &argv[1]);
    //spits_free_ptr(y);

    int hist[] = {5, 5, 5};
    void* z = spits_get_metrics_history((const char**) &argv[1], hist);
    spits_free_ptr(z);

    spits_metrics_delete();


    //
    // for (int i = 0; i< 5; ++i) {
    //     metrics->add_metric("iola", i);
    //     metrics->add_metric("fola", (float) (i+0.5));
    // }
    //
    // auto ielement = metrics->get_metric("iola");
    // if(std::get<0>(ielement))
    //     delete std::get<0>(ielement);
    //
    // ielement = metrics->get_metric("iola");
    // if(std::get<0>(ielement))
    //     delete std::get<0>(ielement);
    //
    // ielement = metrics->get_metric("iola");
    // if(std::get<0>(ielement))
    //     delete std::get<0>(ielement);
    //
    // ielement = metrics->get_metric("iola");
    // if(std::get<0>(ielement))
    //     delete std::get<0>(ielement);
    //
    // ielement = metrics->get_metric("iola");
    // if(std::get<0>(ielement))
    //     delete std::get<0>(ielement);
    //
    // metrics->add_metric("iola", 3289983);
    // ielement = metrics->get_metric("iola");
    //
    // auto felement = metrics->get_metric("fola");
    //
    //
    // if (std::get<1>(ielement) == TYPE_INT) {
    //     IntElement* x = reinterpret_cast<IntElement*>(std::get<0>(ielement));
    //     if (x) {
    //         std::cout << "Value: " << x->value << std::endl;
    //         delete x;
    //     }
    // }
    //
    // if (std::get<1>(felement) == TYPE_FLOAT) {
    //     FloatElement* x = reinterpret_cast<FloatElement*>(std::get<0>(felement));
    //     if (x) {
    //         std::cout << "Value: " << x->value << std::endl;
    //         delete x;
    //     }
    // }
    //
    // for (auto x : metrics->metrics_list())
    //     std::cout << x << std::endl;

    /*std::unique_ptr<IntRing> iring(new IntRing(2));
    std::unique_ptr<FloatRing> fring(new FloatRing(2));

    std::unique_ptr<IntElement> idata1(new IntElement(1234));
    std::unique_ptr<IntElement> idata2(new IntElement(4));
    std::unique_ptr<IntElement> idata3(new IntElement(126));

    std::unique_ptr<FloatElement> fdata1(new FloatElement(1234.5));
    std::unique_ptr<FloatElement> fdata2(new FloatElement(4.5));
    std::unique_ptr<FloatElement> fdata3(new FloatElement(126.5));


    iring->push(std::move(idata1));
    iring->push(std::move(idata2));
    iring->push(std::move(idata3));

    fring->push(std::move(fdata1));
    fring->push(std::move(fdata2));
    fring->push(std::move(fdata3));

    auto x = iring->pop();
    x = iring->pop();
    iring->push(std::move(x));
    x = iring->pop();
    if (x)
        std::cout << x.get()->value << std::endl;

    auto y = fring->pop();
    std::cout << y.get()->value << std::endl;
*/
    return 0;
}
