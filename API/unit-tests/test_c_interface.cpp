#define SPITS_ENTRY_POINT

#include <cassert>
#include <sstream>
#include <string>
#include <exception>
#include <iterator>
#include <algorithm>
#include <metrics.hpp>
#include "test_buffers.hpp"
#include "utils.hpp"

void test_c_interface(void)
{
    const unsigned int n_elements = 5;
    const unsigned int element_size = 15;
    
    void* metrics = spits_metrics_new(n_elements);
        
    assert(spits_get_num_buffers(metrics) == 0);
    spits_set_metric_int(metrics, "Test1", 10);
    assert(spits_get_num_buffers(metrics) == 1);
    spits_metrics_delete(metrics);
    
    metrics = spits_metrics_new(n_elements);
    
    for (unsigned int i = 0; i<n_elements*2; ++i) 
        spits_set_metric_int(metrics, "metric_int", (int) i);
        
    for (unsigned int i = 0; i<n_elements*2; ++i) 
        spits_set_metric_float(metrics, "metric_float", (float) (i + 0.5));
        
    for (unsigned int i = 0; i<n_elements*2; ++i) 
        spits_set_metric_double(metrics, "metric_double", (double) (i + 0.5));
        
    for (unsigned int i = 0; i<n_elements*2; ++i) 
    {
        auto s = generate_random_string(element_size);
        spits_set_metric_string(metrics, "metric_str", (char*) s.c_str());
    }
    
    assert(spits_get_num_buffers(metrics) == 4);
    
    {
        int read_ptr = 0;
        unsigned char* data = reinterpret_cast<unsigned char*>(spits_get_metrics_list(metrics));
        
        assert(read_longlong(data) == 4);
        read_ptr += sizeof(int64_t);
        
        for (int i=0; i<spits_get_num_buffers(metrics); ++i)
        {
            std::string name = read_string(&data[read_ptr]);
            read_ptr += name.size() + 1;
            
            assert(read_longlong(&data[read_ptr]) == n_elements);
            read_ptr += sizeof(int64_t);
                
            if (name == "metric_int")
                assert(read_longlong(&data[read_ptr]) == spits::TYPE_INT);
            else if (name == "metric_float")
                assert(read_longlong(&data[read_ptr]) == spits::TYPE_FLOAT);
            else if (name == "metric_double")
                assert(read_longlong(&data[read_ptr]) == spits::TYPE_DOUBLE);
            else if (name == "metric_str")
                assert(read_longlong(&data[read_ptr]) == spits::TYPE_BYTES);
            else
                assert("Invalid metric name" && false);
            
            read_ptr += sizeof(int64_t);
        }
        
        spits_free_ptr(data);
    }
    
    const char* str = "Hello1";
    spits_set_metric_string(metrics, "metric_str", (char*) str);
    
    {
        int read_ptr = 0;
        const char* valid_names[] = {"metric_int", "metric_float", "metric_double", "metric_str", NULL};
        unsigned char* data = reinterpret_cast<unsigned char*>(spits_get_metrics_last_values(metrics, valid_names));
        
        // metric int parse
        {
            //Read metric value type
            assert(read_longlong(&data[read_ptr]) == spits::TYPE_INT);
            read_ptr += sizeof(int64_t);
            //Read metric int value
            assert(read_longlong(&data[read_ptr]) == n_elements*2-1);
            read_ptr += sizeof(int64_t);
            // Seconds
            assert(read_longlong(&data[read_ptr]) > 0);
            read_ptr += sizeof(int64_t);
            // Nanoseconds
            assert(read_longlong(&data[read_ptr]) > 0);
            read_ptr += sizeof(int64_t);
            // Sequence number
            assert(read_longlong(&data[read_ptr]) == n_elements*2-1);
            read_ptr += sizeof(int64_t);
        }
        
        // metric float parse
        {
            //Read metric value type
            assert(read_longlong(&data[read_ptr]) == spits::TYPE_FLOAT);
            read_ptr += sizeof(int64_t);
            //Read metric float value
            assert(read_float(&data[read_ptr]) == n_elements*2-1+0.5);
            read_ptr += sizeof(float);
            // Seconds
            assert(read_longlong(&data[read_ptr]) > 0);
            read_ptr += sizeof(int64_t);
            // Nanoseconds
            assert(read_longlong(&data[read_ptr]) > 0);
            read_ptr += sizeof(int64_t);
            // Sequence number
            assert(read_longlong(&data[read_ptr]) == n_elements*2-1);
            read_ptr += sizeof(int64_t);
        }
        
        // metric double parse
        {
            //Read metric value type
            assert(read_longlong(&data[read_ptr]) == spits::TYPE_DOUBLE);
            read_ptr += sizeof(int64_t);
            //Read metric double value
            assert(read_double(&data[read_ptr]) == n_elements*2-1+0.5);
            read_ptr += sizeof(double);
            // Seconds
            assert(read_longlong(&data[read_ptr]) > 0);
            read_ptr += sizeof(int64_t);
            // Nanoseconds
            assert(read_longlong(&data[read_ptr]) > 0);
            read_ptr += sizeof(int64_t);
            // Sequence number
            assert(read_longlong(&data[read_ptr]) == n_elements*2-1);
            read_ptr += sizeof(int64_t);
        }
        
        // metric string parse
        {
            //Read metric value type
            assert(read_longlong(&data[read_ptr]) == spits::TYPE_BYTES);
            read_ptr += sizeof(int64_t);
            
            // Read string size
            std::string str_name(str);
            uint64_t str_size = read_longlong(&data[read_ptr]);
            assert(str_name.size() == str_size-1);
            read_ptr += sizeof(int64_t);
            
            // Read String
            std::string value = read_string(&data[read_ptr]);
            assert(str_name == value);
            read_ptr += value.size()+1;

            // Seconds
            assert(read_longlong(&data[read_ptr]) > 0);
            read_ptr += sizeof(int64_t);
            // Nanoseconds
            assert(read_longlong(&data[read_ptr]) > 0);
            read_ptr += sizeof(int64_t);
            // Sequence number
            assert(read_longlong(&data[read_ptr]) == n_elements*2);
            read_ptr += sizeof(int64_t);
        }
        
        
        spits_free_ptr(data);
    }
    
    
    {
        int read_ptr = 0;
        const char* valid_names[] = {"metric_int", "metric_float", "metric_double", "metric_str", NULL};
        int sizes[] = {10, 10, 10, 10};
        unsigned char* data = reinterpret_cast<unsigned char*>(spits_get_metrics_history(metrics, valid_names, sizes));
        
        // metric int parse
        {
            //Read metric value type
            assert(read_longlong(&data[read_ptr]) == spits::TYPE_INT);
            read_ptr += sizeof(int64_t);
            
            // Sequence no
            int64_t seq_no = read_longlong(&data[read_ptr]);
            assert(seq_no == n_elements);
            read_ptr += sizeof(int64_t);
            
            // Number of elements
            int64_t no_elements = read_longlong(&data[read_ptr]);
            assert(no_elements == n_elements);
            read_ptr += sizeof(int64_t);
            
            for (int64_t i = 0; i<no_elements; ++i)
            {
                // value
                assert(read_longlong(&data[read_ptr]) == (int64_t) n_elements+i);
                read_ptr += sizeof(int64_t);
                // Seconds
                assert(read_longlong(&data[read_ptr]) > 0);
                read_ptr += sizeof(int64_t);
                // Nanoseconds
                assert(read_longlong(&data[read_ptr]) > 0);
                read_ptr += sizeof(int64_t);
            }
        }
        
        // metric float parse
        {
            //Read metric value type
            assert(read_longlong(&data[read_ptr]) == spits::TYPE_FLOAT);
            read_ptr += sizeof(int64_t);
            
            // Sequence no
            int64_t seq_no = read_longlong(&data[read_ptr]);
            assert(seq_no == n_elements);
            read_ptr += sizeof(int64_t);
            
            // Number of elements
            int64_t no_elements = read_longlong(&data[read_ptr]);
            assert(no_elements == n_elements);
            read_ptr += sizeof(int64_t);
            
            for (int64_t i = 0; i<no_elements; ++i)
            {
                // float
                assert(read_float(&data[read_ptr]) == (float) n_elements+i+0.5);
                read_ptr += sizeof(float);
                // Seconds
                assert(read_longlong(&data[read_ptr]) > 0);
                read_ptr += sizeof(int64_t);
                // Nanoseconds
                assert(read_longlong(&data[read_ptr]) > 0);
                read_ptr += sizeof(int64_t);
            }
        }
        
        // metric double parse
        {
            //Read metric value type
            assert(read_longlong(&data[read_ptr]) == spits::TYPE_DOUBLE);
            read_ptr += sizeof(int64_t);
            
            // Sequence no
            int64_t seq_no = read_longlong(&data[read_ptr]);
            assert(seq_no == n_elements);
            read_ptr += sizeof(int64_t);
            
            // Number of elements
            int64_t no_elements = read_longlong(&data[read_ptr]);
            assert(no_elements == n_elements);
            read_ptr += sizeof(int64_t);
            
            for (int64_t i = 0; i<no_elements; ++i)
            {
                // double
                assert(read_double(&data[read_ptr]) == (double) n_elements+i+0.5);
                read_ptr += sizeof(double);
                // Seconds
                assert(read_longlong(&data[read_ptr]) > 0);
                read_ptr += sizeof(int64_t);
                // Nanoseconds
                assert(read_longlong(&data[read_ptr]) > 0);
                read_ptr += sizeof(int64_t);
            }
        }
        
        // metric str parse
        {
            //Read metric value type
            assert(read_longlong(&data[read_ptr]) == spits::TYPE_BYTES);
            read_ptr += sizeof(int64_t);
            
            // Sequence no
            int64_t seq_no = read_longlong(&data[read_ptr]);
            assert(seq_no == n_elements+1);
            read_ptr += sizeof(int64_t);
            
            // Number of elements
            int64_t no_elements = read_longlong(&data[read_ptr]);
            assert(no_elements == n_elements);
            read_ptr += sizeof(int64_t);
            
            for (int64_t i = 0; i<no_elements-1; ++i)
            {
                // Read string size
                uint64_t str_size = read_longlong(&data[read_ptr]);
                assert(str_size-1 == element_size);
                read_ptr += sizeof(int64_t);
            
                // Read String and discard
                read_ptr += str_size;
                
                // Seconds
                assert(read_longlong(&data[read_ptr]) > 0);
                read_ptr += sizeof(int64_t);
                // Nanoseconds
                assert(read_longlong(&data[read_ptr]) > 0);
                read_ptr += sizeof(int64_t);
            }
            
            //Last string...
            // Read string size
            std::string str_name(str);
            uint64_t str_size = read_longlong(&data[read_ptr]);
            assert(str_name.size() == str_size-1);
            read_ptr += sizeof(int64_t);
            
            // Read String
            std::string value = read_string(&data[read_ptr]);
            assert(str_name == value);
            read_ptr += value.size()+1;
                
            // Seconds
            assert(read_longlong(&data[read_ptr]) > 0);
            read_ptr += sizeof(int64_t);
            // Nanoseconds
            assert(read_longlong(&data[read_ptr]) > 0);
            read_ptr += sizeof(int64_t);
        }
        
        
        spits_free_ptr(data);
    }
    
    
    // NULL values test
    assert(spits_get_metrics_last_values(metrics, NULL) == NULL);
    assert(spits_get_metrics_history(metrics, NULL, NULL) == NULL);
    
    // Invalid values
    const char* invalid_names[] = {"metric_str", "Nop", NULL};
    int sizes[] = {-1, -2};
    
    // Invalids with data
    assert(spits_get_metrics_last_values(metrics, invalid_names) == NULL);
    assert(spits_get_metrics_history(metrics, invalid_names, NULL) == NULL);
    assert(spits_get_metrics_history(metrics, NULL, sizes) == NULL);
    assert(spits_get_metrics_history(metrics, invalid_names, sizes) == NULL);
    
    // Invalids without data
    spits_metrics_reset(metrics);
    assert(spits_get_num_buffers(metrics) == 0);
    assert(spits_get_metrics_list(metrics) == NULL);
    assert(spits_get_metrics_last_values(metrics, invalid_names) == NULL);
    assert(spits_get_metrics_history(metrics, invalid_names, sizes) == NULL);
    
    // Finalize
    spits_metrics_delete(metrics);
}

