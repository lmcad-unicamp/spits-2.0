#include <iostream>
#include "test_rings.hpp"
#include "test_buffers.hpp"
#include "test_metrics.hpp"
#include "test_c_interface.hpp"

int main(int argc, char** argv){
    std::cout << "Testing integer rings... ";
    test_integer_ring();
    std::cout << "OK" << std::endl;
    
    std::cout << "Testing array rings... ";
    test_bytes_ring();
    std::cout << "OK" << std::endl;
    
    std::cout << "Testing integer buffer... ";
    test_integer_buffer();
    std::cout << "OK" << std::endl;
    
    std::cout << "Testing bytes buffer... ";
    test_bytes_buffer();
    std::cout << "OK" << std::endl;
    
    std::cout << "Testing metric buffer interface... ";
    test_metric_buffer_interface();
    std::cout << "OK" << std::endl;
    
    std::cout << "Testing c interface... ";
    test_c_interface();
    std::cout << "OK" << std::endl;
    
    return 0;
}
