#include <cassert>
#include <cstring>
#include <exception>
#include <metrics.hpp>
#include "test_rings.hpp"
#include "utils.hpp"

void test_integer_ring(void){
    const unsigned int n_elements = 5;
    
    if (n_elements < 2)
        throw std::runtime_error("Number of element should be higher than 2");
    
    // Create an integer buffer for n_elements elements
    spits::ring_metric_int int_ring(n_elements);
    
    // Check empty
    assert(int_ring.empty());
    
    
    //Push all -1 elements
    for (unsigned int i = 0; i<n_elements-1; ++i)
        int_ring.push(new spits::metric_element_int(i, i));
        
    // Is length correct, not empty and is not full?
    assert(int_ring.length() == n_elements-1);
    assert(!int_ring.full());
    assert(!int_ring.empty());
    
    
    // Add more one element
    int_ring.push(new spits::metric_element_int(10000, 100));
    // Is length correct, not empty and is full?
    assert(int_ring.length() == n_elements);
    assert(int_ring.full());
    assert(!int_ring.empty());
    
    
    // Override with n_elements more elements 
    for (unsigned int i = 0; i<n_elements; ++i)
        int_ring.push(new spits::metric_element_int(i, i));
     // Is length correct, not empty and is full?   
    assert(int_ring.length() == n_elements);
    assert(int_ring.full());
    assert(!int_ring.empty());
    
    
    // Get the last element
    auto element = int_ring.get_back_element();
    assert(element->get_value() == 0);
    
    // Get the recent element
    auto element_2 = int_ring.get_front_element();
    assert(element_2->get_value() == n_elements-1);
    
    // Check if is different
    assert(element != element_2);
    
    
    // Pop n_element-1 elements
   for (unsigned int i = 0; i<n_elements-1; ++i)
        delete int_ring.pop();
    // Is length correct, not empty and is not full?  
    assert(int_ring.length() == 1);
    assert(!int_ring.full());
    assert(!int_ring.empty());
    
    
    // Pop the last element
    element = int_ring.pop();
    // n_elements-1?
    assert(element->get_value() == n_elements-1);
    // Length==0, not full and empty?
    assert(!int_ring.length());
    assert(!int_ring.full());
    assert(int_ring.empty());
    delete element;
    
    // Pop with empty
    element = int_ring.pop();
    // NULL?
    assert(element == nullptr);
    // Length==0, not full and empty?
    assert(!int_ring.length());
    assert(!int_ring.full());
    assert(int_ring.empty());    
}


void test_bytes_ring(void) {
    const unsigned int n_elements = 5;
    const unsigned int element_size = 16;
    const std::string c_element1("test001");
    const std::string c_element2("test000000000002");
    const std::string c_element3("test03");
    
    if (n_elements < 2)
        throw std::runtime_error("Number of element should be higher than 2");
    
    // Create an integer buffer for n_elements elements
    spits::ring_metric_bytes bytes_ring(n_elements);
    
    // Check empty
    assert(bytes_ring.empty());
    
    //Push all -1 elements
    for (unsigned int i = 0; i<n_elements-1; ++i){
        auto s = generate_random_string(element_size);
        bytes_ring.push(new spits::metric_element_bytes((unsigned char*) s.c_str(), s.size()+1, i));
    }
        
    // Is length correct, not empty and is not full?
    assert(bytes_ring.length() == n_elements-1);
    assert(!bytes_ring.full());
    assert(!bytes_ring.empty());  
    
    {
        // Add more one element
        auto s = generate_random_string(element_size); 
        bytes_ring.push(new spits::metric_element_bytes((unsigned char*) s.c_str(), s.size()+1, 100));
        // Is length correct, not empty and is full?
        assert(bytes_ring.length() == n_elements);
        assert(bytes_ring.full());
        assert(!bytes_ring.empty());
    }
    
    
    // Override with n_elements more elements 
    for (unsigned int i = 0; i<n_elements*2; ++i) {
        auto s = generate_random_string(element_size);
        bytes_ring.push(new spits::metric_element_bytes((unsigned char*) s.c_str(), s.size()+1, i));
    }
    // Is length correct, not empty and is full?   
    assert(bytes_ring.length() == n_elements);
    assert(bytes_ring.full());
    assert(!bytes_ring.empty());
    

    // Pop n_element-1 elements
    for (unsigned int i = 0; i<n_elements-1; ++i)
        delete bytes_ring.pop();
    // Is length correct, not empty and is not full?  
    assert(bytes_ring.length() == 1);
    assert(!bytes_ring.full());
    assert(!bytes_ring.empty());
    
    
    // Pop the last element
    delete bytes_ring.pop();
    // Length==0, not full and empty?
    assert(!bytes_ring.length());
    assert(!bytes_ring.full());
    assert(bytes_ring.empty());
    
    // Pop with empty --> NULL
    assert(bytes_ring.pop() == nullptr);
    // Length==0, not full and empty?
    assert(!bytes_ring.length());
    assert(!bytes_ring.full());
    assert(bytes_ring.empty());
    
    for (unsigned i=0; i<n_elements*2; ++i)
        bytes_ring.push(new spits::metric_element_bytes((unsigned char*) c_element1.c_str(), c_element1.size()+1, i));
    
    for (unsigned i=0; i<n_elements*2; ++i) {
        auto x = bytes_ring.pop();
        if(x) delete x;
        bytes_ring.push(new spits::metric_element_bytes((unsigned char*) c_element1.c_str(), c_element1.size()+1, i));
        bytes_ring.push(new spits::metric_element_bytes((unsigned char*) c_element1.c_str(), c_element1.size()+1, i));
    }
    
    for (unsigned i=0; i<n_elements*2; ++i) {
        auto x = bytes_ring.pop();
        if(x) delete x;
        auto y = bytes_ring.pop();
        if(y) delete y;
        bytes_ring.push(new spits::metric_element_bytes((unsigned char*) c_element1.c_str(), c_element1.size()+1, i));
    }
    
    {
        auto x = bytes_ring.pop();
        assert(x);
        assert(c_element1 == (const char*) x->get_value());
        delete x;
    }
    
    {    
        for (unsigned i=0; i<n_elements; ++i)
            bytes_ring.push(new spits::metric_element_bytes((unsigned char*) c_element1.c_str(), c_element1.size()+1, i));
        
        bytes_ring.push(new spits::metric_element_bytes((unsigned char*) c_element2.c_str(), c_element2.size()+1, n_elements));
        bytes_ring.push(new spits::metric_element_bytes((unsigned char*) c_element3.c_str(), c_element3.size()+1, n_elements+1));
        
        auto x = bytes_ring.pop();
        assert(x);
        assert(c_element1 == (const char*) x->get_value());
        delete x;
    }
    
    for (unsigned i=0; i<n_elements+1; ++i) {
        auto x = bytes_ring.pop();
        if(x) delete x;
    }
    
    bytes_ring.push(new spits::metric_element_bytes((unsigned char*) c_element1.c_str(), c_element1.size()+1, 0));
    bytes_ring.push(new spits::metric_element_bytes((unsigned char*) c_element2.c_str(), c_element2.size()+1, 1));
    bytes_ring.push(new spits::metric_element_bytes((unsigned char*) c_element3.c_str(), c_element3.size()+1, 2));
    
    
    {
        // Get the last element
        auto x = bytes_ring.get_back_element();
        assert(c_element1 == (const char*) x->get_value());
        
        // Get the recent element
        auto y = bytes_ring.get_front_element();
        assert(c_element3 == (const char*) y->get_value());
    }
    
    {
        auto x =  bytes_ring.get_element(0);
        assert(c_element1 == (const char*) x->get_value());
        
        auto y =  bytes_ring.get_element(1);
        assert(c_element2 == (const char*) y->get_value());
        
        auto z =  bytes_ring.get_element(2);
        assert(c_element3 == (const char*) z->get_value());
        
        assert(bytes_ring.get_element(-1) == nullptr);
    }
}
