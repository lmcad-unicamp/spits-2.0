#include <cassert>
#include <cstring>
#include <exception>
#include <metrics.hpp>
#include "test_metrics.hpp"
#include "utils.hpp"

void test_metric_buffer_interface(void) {
    const unsigned int n_elements = 5;
    const unsigned int element_size = 15;
    
    if (n_elements < 2)
        throw std::runtime_error("Number of element should be higher than 2");
        
    spits::metrics metrics = spits::metrics(n_elements);
    
    for (unsigned int i = 0; i<n_elements*2; ++i) 
        metrics.add_metric("metric_int", (int) i);
        
    for (unsigned int i = 0; i<n_elements*2; ++i) 
        metrics.add_metric("metric_float", (float) (i + 0.5));
        
    for (unsigned int i = 0; i<n_elements*2; ++i) 
        metrics.add_metric("metric_double", (double) (i + 0.5));
        
    for (unsigned int i = 0; i<n_elements*2; ++i) {
        auto s = generate_random_string(element_size);
        metrics.add_metric("metric_str", (const char*) s.c_str());
    }
    
    const char* strs[] = {"Hello1 xxx", "Hello 2 - testing", "Hello 3"};
    metrics.add_metric("metric_str", strs[0]);
    metrics.add_metric("metric_str", strs[1]);
    metrics.add_metric("metric_str", strs[2]);

    {
        auto metric_list = metrics.list();
        assert(metric_list.size() == 4);
        for (auto metric : metric_list) 
        {
            auto name = std::string(std::get<0>(metric));
            uint64_t size = std::get<1>(metric);
            int type = std::get<2>(metric);
            
            if (name == "metric_int") 
                assert(type == spits::TYPE_INT);
            else if (name == "metric_float") 
                assert(type == spits::TYPE_FLOAT);
            else if (name == "metric_double") 
                assert(type == spits::TYPE_DOUBLE);
            else if (name == "metric_str") 
                assert(type == spits::TYPE_BYTES);
            else
                assert(false);
                
            assert(size == n_elements);
        }
    }
    
    {
        // Get the integer elements
        std::vector<std::tuple<std::string, int>> metrics_list;
        metrics_list.push_back({"metric_int", 0});
        
        auto metric_values = metrics.last_values(metrics_list);
        for (auto it = metric_values.begin(); it != metric_values.end(); it++) 
        {
            assert(it->first == "metric_int");
            assert(it->second.size() == 0);
        }
        metrics.delete_values_map(metric_values);
    }
    
    {
        // Get the integer elements
        std::vector<std::tuple<std::string, int>> metrics_list;
        metrics_list.push_back({"metric_int", n_elements*10});
        
        auto metric_values = metrics.last_values(metrics_list);
        for (auto it = metric_values.begin(); it != metric_values.end(); it++) 
        {
            assert(it->first == "metric_int");
            assert(it->second.size() == n_elements);
            for (auto i = 0; i<(int)it->second.size(); ++i) 
                assert(reinterpret_cast<spits::metric_element_int*>(it->second.at(i))->get_value() == (int) n_elements+i);
        }
        metrics.delete_values_map(metric_values);
    }
    
    {
        // Get the integer elements
        std::vector<std::tuple<std::string, int>> metrics_list;
        metrics_list.push_back({"metric_int", 2});
        
        auto metric_values = metrics.last_values(metrics_list, false);
        for (auto it = metric_values.begin(); it != metric_values.end(); it++) 
        {
            assert(it->first == "metric_int");
            assert(it->second.size() == 2);
            for (auto i = 0; i<(int)it->second.size(); ++i) 
                assert(reinterpret_cast<spits::metric_element_int*>(it->second.at(i))->get_value() == (int) n_elements*2-2+i);
        }
        metrics.delete_values_map(metric_values);
    }
    
    {
        // Get the integer elements
        std::vector<std::tuple<std::string, int>> metrics_list;
        metrics_list.push_back({"metric_int", n_elements*10});
        
        auto metric_values = metrics.last_values(metrics_list, false);
        for (auto it = metric_values.begin(); it != metric_values.end(); it++) 
        {
            assert(it->first == "metric_int");
            assert(it->second.size() == n_elements);
            for (auto i = 0; i<(int)it->second.size(); ++i) 
                assert(reinterpret_cast<spits::metric_element_int*>(it->second.at(i))->get_value() == (int) n_elements+i);
        }
        metrics.delete_values_map(metric_values);
    }
    
    
    {
        // Get all type elements
        std::vector<std::tuple<std::string, int>> metrics_list;
        metrics_list.push_back({"metric_int", 2});
        metrics_list.push_back({"metric_float", 2});
        metrics_list.push_back({"metric_double", 2});
        metrics_list.push_back({"metric_str", 2});
        
        auto metric_values = metrics.last_values(metrics_list);
        for (auto it = metric_values.begin(); it != metric_values.end(); it++) 
        {
            assert(it->first == "metric_int" || it->first == "metric_str" || it->first == "metric_float" || it->first == "metric_double");
            assert(it->second.size() == 2);
            for (auto i = 0; i<(int)it->second.size(); ++i) 
            {
                if (it->first == "metric_int")
                    assert(reinterpret_cast<spits::metric_element_int*>(it->second.at(i))->get_value() == (int) n_elements*2-2+i);
    
                else if (it->first == "metric_float")
                    assert(reinterpret_cast<spits::metric_element_float*>(it->second.at(i))->get_value() == (float) n_elements*2-2+i+0.5);

                else if (it->first == "metric_double")
                    assert(reinterpret_cast<spits::metric_element_double*>(it->second.at(i))->get_value() == (double) n_elements*2-2+i+0.5);

                else if (it->first == "metric_str")
                    assert(std::string(strs[i+1]) == (const char*) reinterpret_cast<spits::metric_element_bytes*>(it->second.at(i))->get_value());
                else
                    assert(false);
            }
        }
        metrics.delete_values_map(metric_values);
    }
}
