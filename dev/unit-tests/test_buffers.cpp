#include <cassert>
#include <cstring>
#include <exception>
#include <metrics.hpp>
#include "test_buffers.hpp"
#include "utils.hpp"

void test_integer_buffer(void){
    const unsigned int n_elements = 5;
    
    if (n_elements < 2)
        throw std::runtime_error("Number of element should be higher than 2");
   
    spits::int_metric_buffer int_buffer(n_elements);
    
    assert(int_buffer.get_buffer_type() == spits::TYPE_INT);
    assert(int_buffer.get_buffer_capacity() == n_elements);
    assert(int_buffer.get_buffer_sequence_counter() == 0);
    
    for (unsigned int i = 0; i<n_elements*2; ++i){
        int_buffer.add_element(i);
    }
    
    assert(int_buffer.get_buffer_sequence_counter() == n_elements*2);
    
    {
        auto element = int_buffer.get_front_element();
        assert(element->get_value() == n_elements*2-1); 
        delete element;
    }
    
    {
        auto element = int_buffer.get_back_element();
        assert(element->get_value() == n_elements); 
        delete element;
    }
    
    for (unsigned int i = 0; i<n_elements; ++i) 
    {
        auto element = int_buffer.get_element(i);
        assert(element->get_value() == (int)(n_elements+i)); 
        delete element;
    }
}

void test_bytes_buffer(void){
    const unsigned int n_elements = 5;
    const unsigned int element_size = 16;
    const std::string c_element1("test001");
    const std::string c_element2("test000000000002");
    const std::string c_element3("test03");
    
    if (n_elements < 2)
        throw std::runtime_error("Number of element should be higher than 2");
   
    spits::bytes_metric_buffer bytes_buffer(n_elements);
    
    assert(bytes_buffer.get_buffer_type() == spits::TYPE_BYTES);
    assert(bytes_buffer.get_buffer_capacity() == n_elements);
    assert(bytes_buffer.get_buffer_sequence_counter() == 0);
    
    for (unsigned int i = 0; i<n_elements*2; ++i){      
        auto s = generate_random_string(element_size);
        bytes_buffer.add_element((unsigned char*) s.c_str(), s.size()+1);
    }
    
    assert(bytes_buffer.get_buffer_sequence_counter() == n_elements*2);
    
    for (unsigned int i = 0; i<n_elements*2; ++i)
        bytes_buffer.add_element((unsigned char*) c_element1.c_str(), c_element1.size()+1);
    
    bytes_buffer.add_element((unsigned char*) c_element2.c_str(), c_element2.size()+1);
    bytes_buffer.add_element((unsigned char*) c_element3.c_str(), c_element3.size()+1);
    
    assert(bytes_buffer.get_buffer_sequence_counter() == n_elements*4+2);
    
    {
        auto x = bytes_buffer.get_front_element();
        assert(x);
        assert(c_element3 == (const char*) x->get_value());
        delete x;
    }
    
    {
        auto x = bytes_buffer.get_back_element();
        assert(x);
        assert(c_element1 == (const char*) x->get_value());
        delete x;
    }
    
    {
        auto x = bytes_buffer.get_element(n_elements-2);
        assert(x);
        assert(c_element2 == (const char*) x->get_value());
        delete x;
    }
}
