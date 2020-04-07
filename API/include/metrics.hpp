#ifndef __METRICS_HPP__
#define __METRICS_HPP__

#include "metrics.h"
#include "stream.hpp"

#include <iostream>
#include <cstddef>
#include <memory>
#include <cstdbool>
#include <map>
#include <exception>
#include <tuple>
#include <vector>
#include <sstream>
#include <cstring>
#include <algorithm>


#ifndef DEFAULT_BUFFER_SIZE
#define DEFAULT_BUFFER_SIZE 10
#endif

namespace spits
{
enum TYPE {
    TYPE_INT = 0,
    TYPE_FLOAT,
    TYPE_DOUBLE,
    TYPE_BYTES
};

/*
 *  Metric Elements
 */
class element {
protected:
    int element_type;
    unsigned int seq_no;
    struct timespec timestamp;

public:
    element(int element_type, unsigned int sequence) : 
                element_type(element_type), seq_no(sequence) 
    {
    }
    
    element(element& another)
    {
        this->element_type = another.element_type;
        this->seq_no = another.seq_no;
        std::memcpy((void*) &this->timestamp, (void*) &another.timestamp, sizeof(struct timespec));
    }

    virtual ~element() 
    {
    }

    struct timespec& get_element_time(void) 
    {
        return this->timestamp;
    }

    int get_element_type(void) 
    {
        return this->element_type;
    }
    
    unsigned int get_element_sequence(void)
    {
        return this->seq_no;
    }
};


template <typename ElementType, int TypeName>
class metric_element : public element {
    ElementType value;
    
public:
    metric_element(ElementType value, unsigned int sequence) :
        element(TypeName, sequence), value(value) 
    {
        clock_gettime(CLOCK_REALTIME, &timestamp);
    }
    
    metric_element(metric_element& another) : element(another)
    {
        this->value = another.value;
    }
    
    virtual ~metric_element() 
    {
    }
    
    ElementType get_value(void)
    {
        return this->value;
    }
};


template <typename ElementType, int TypeName>
class metric_element_array : public element {
    ElementType* value;
    unsigned int size;

public:
    metric_element_array(ElementType* ptr, unsigned int size, unsigned int sequence) : 
        element(TypeName, sequence), value(new ElementType[size]), size(size) 
    {
        clock_gettime(CLOCK_REALTIME, &timestamp);
        std::memcpy(this->value, ptr, size);
    }
    
    metric_element_array(metric_element_array& another) : 
        element(another)
    {
        this->size = another.size;
        this->value = new ElementType[another.size];
        std::memcpy(this->value, another.value, another.size);
    }

    virtual ~metric_element_array() 
    {
        delete[] value;
    }
    
    ElementType* get_value(void)
    {
        return this->value;
    }
    
    int get_size(void)
    {
        return this->size;
    }
};


/*
 *  Defining default metric element types
 */
typedef metric_element<int, TYPE_INT> metric_element_int;
typedef metric_element<float, TYPE_FLOAT> metric_element_float;
typedef metric_element<double, TYPE_DOUBLE> metric_element_double;
typedef metric_element_array<unsigned char, TYPE_BYTES> metric_element_bytes;


/*
 *  Ring Implementation for Metric Element Types
 */
template <typename T>
class ring {
private:
    T* m_data;
    int _head, _tail, _capacity, _size;
    
public:
    explicit ring(int _capacity = 1) : 
        m_data(new T[_capacity]()), _head(0), _tail(0), 
        _capacity(_capacity), _size(0)
    {
    }

    ~ring() 
    {
        reset();
    }
    
    void reset(void)
    {
        while(!empty()) delete pop();
        delete[] m_data;
    }

    void push(T element) 
    {
        if (full()) 
            delete pop();
            
        m_data[_head] = element;
        _head = (_head+1)%_capacity;
        _size++;
    }
    
    T pop(void) 
    {
        if (empty()) 
            return nullptr;
        
        auto m_ptr = m_data[_tail];
        _tail = (_tail+1)%_capacity;
        _size--;
        return m_ptr;
    }

    T get_front_element(void) 
    {
        int head_element = (_head-1)%_capacity;
        if(head_element < 0)
            head_element = _capacity-1;
            
        return empty()? nullptr : m_data[head_element];
    }
    
    T get_back_element(void)
    {
        int tail_element = _tail;
        return empty()? nullptr : m_data[tail_element];
    }
    
    T get_element(int index)
    {        
        if (empty() || index < 0 || index >= _size)
            return nullptr;
            
        return  m_data[(_tail + index) % _capacity];
    }

    bool full(void) 
    {
        return _size == _capacity;
    }

    bool empty(void) 
    {
        return _size == 0;
    }

    int length(void) 
    {
        return _size;
    }
};



/*
 *  Defining Ring for Integer, Float and Bytes Types
 */
typedef ring<metric_element_int*> ring_metric_int;
typedef ring<metric_element_float*> ring_metric_float;
typedef ring<metric_element_double*> ring_metric_double;
typedef ring<metric_element_bytes*> ring_metric_bytes;



/*
 *  Metric Buffers  
 */

class generic_buffer {
protected:
    int buffer_type;
    unsigned int buffer_capacity;
    unsigned int buffer_sequence_counter;
    
public:
    generic_buffer(int buffer_type, unsigned int buffer_capacity) : 
        buffer_type(buffer_type), buffer_capacity(buffer_capacity), buffer_sequence_counter(0) 
    {
        if (buffer_capacity <= 0)
            throw std::runtime_error("Buffer length must be larger than 1");
    };

    virtual ~generic_buffer() 
    { 
    }

    int get_buffer_type(void) 
    {
        return buffer_type;
    }

    int get_buffer_capacity(void) 
    {
        return buffer_capacity;
    }

    int get_buffer_sequence_counter(void) 
    {
        return buffer_sequence_counter;
    }
};


template <typename Type, int TypeName, typename MetricElementType, typename RingType>
class metric_element_buffer : public generic_buffer {
    RingType* ring;

public:
    metric_element_buffer(int buffer_capacity) : generic_buffer(TypeName, buffer_capacity),
        ring(new RingType(buffer_capacity)) 
    { 
    }

    ~metric_element_buffer() 
    {
        if (ring)
            delete ring;
    }

    virtual void add_element(Type element) 
    {
        ring->push(new MetricElementType(element, buffer_sequence_counter++));
    }

    MetricElementType* get_element(int index) {
        MetricElementType* got_element = ring->get_element(index);
        if (!got_element)
            return nullptr;
    
        return new MetricElementType(*got_element);
    }
    
    MetricElementType* get_front_element(void) {
        MetricElementType* got_element = ring->get_front_element();
        if (!got_element)
            return nullptr;
        return new MetricElementType(*got_element);
    }
    
    MetricElementType* get_back_element(void) {
        MetricElementType* got_element = ring->get_back_element();
        if (!got_element)
            return nullptr;
        return new MetricElementType(*got_element);
    }
};

template <typename Type, int TypeName, typename MetricElementType, typename RingType>
class metric_element_array_buffer : public generic_buffer {
    RingType* ring;

public:
    metric_element_array_buffer(int buffer_capacity) : generic_buffer(TypeName, buffer_capacity),
        ring(new RingType(buffer_capacity)) 
    { 
    }

    ~metric_element_array_buffer() 
    {
        if (ring)
            delete ring;
    }

    virtual void add_element(Type* element, unsigned int size) 
    {
        ring->push(new MetricElementType(element, size, buffer_sequence_counter++));
    }

    MetricElementType* get_element(int index) {
        MetricElementType* got_element = ring->get_element(index);
        if (!got_element)
            return nullptr;
    
        return new MetricElementType(*got_element);
    }
    
    MetricElementType* get_front_element(void) {
        MetricElementType* got_element = ring->get_front_element();
        if (!got_element)
            return nullptr;
        return new MetricElementType(*got_element);
    }
    
    MetricElementType* get_back_element(void) {
        MetricElementType* got_element = ring->get_back_element();
        if (!got_element)
            return nullptr;
        return new MetricElementType(*got_element);
    }
};

typedef metric_element_buffer<int, TYPE_INT, metric_element_int, ring_metric_int> int_metric_buffer;
typedef metric_element_buffer<float, TYPE_FLOAT, metric_element_float, ring_metric_float> float_metric_buffer;
typedef metric_element_buffer<double, TYPE_DOUBLE, metric_element_double, ring_metric_double> double_metric_buffer;
typedef metric_element_array_buffer<unsigned char, TYPE_BYTES, metric_element_bytes, ring_metric_bytes> bytes_metric_buffer;




///*
 //*  Final Metric Buffer
 //*/
class metrics 
{
public:
    /**
     *  Create a new metrics class, which holds multiple buffers. Each buffer is associated with a unique name.
     *  @param default_size The default buffer size of each buffer. Each created buffer will hold this amount of elements
     */
    metrics(int capacity=DEFAULT_BUFFER_SIZE) : default_capacity(capacity) 
    {
    }

    /**
     *  Deletes all metric buffers
     */
    ~metrics() 
    {
        this->reset();
    }

    /**
     *  Reset all metric buffer and their elements
     */
    void reset(void)
    {
        // Delete all metric buffers in the map
        for (auto it = metric_buffers.begin(); it != metric_buffers.end(); it++) 
        {
            if (it->second)
                delete it->second;
        }

        metric_buffers.clear();
    }

    /**
     *  Get default buffer capacity
     */
    unsigned int get_default_capacity(void)
    {
        return this->default_capacity;
    }
    
    /**
     *  Set capacity for new created buffers 
     *  @param capacity New capacity
     */
    void set_default_capacity(unsigned int capacity)
    {
        this->default_capacity = capacity;
    }

    /**
     *  Get number of registered metric buffers 
     */
    unsigned int get_num_metric_buffers(void)
    {
        return metric_buffers.size();
    }

    /**
     *  Get the name of all metrics buffers registered
     *  @return A vector of triples, where each triple consists of: (the metric name, the metric capacity, the metric type) 
     */
    std::vector<std::tuple<std::string, int, int>> list(void) 
    {
        std::vector<std::tuple<std::string, int, int>> metrics_list;
        for (auto const& key : this->metric_buffers)
            metrics_list.push_back({key.first, (uint64_t) key.second->get_buffer_capacity(), key.second->get_buffer_type()});

        return metrics_list;
    }

    /**
     *  Get the last elements of some named buffers
     * 
     *  @param metrics_list Vector of tuples with the name of the metrics. Each tuple consists of: (the buffer name, number of elements to get)
     *  @param from_last If true, the lasts values from the recent one is got else the last values from the last one to the newest is got
     *  @return A map where the keys are the buffer names and the values are a vector of elements  
     */
    std::map<std::string, std::vector<element*>> last_values(std::vector<std::tuple<std::string, int>> metrics_list, bool from_last = false) 
    {
        // Create a metric map
        std::map<std::string, std::vector<element*> > metrics_map;

        // Iterate over metric list tuples <buffer_name, requested_size>
        for(auto const& metric : metrics_list) 
        {
            std::vector<element*> element_buffer;

            // Search in the buffer map for the named buffer
            auto M = metric_buffers.find(std::get<0>(metric));
            // If named buffer does not exists, reset and return a clear buffer
            if (M == metric_buffers.end()) 
            {
                delete_values_map(metrics_map);
                metrics_map.clear();
                return metrics_map;
            }

            //If named buffer found, get buffer
            generic_buffer* m = M->second;

            // Calculate the valid requested size
            int requested_size = std::get<1>(metric) > m->get_buffer_capacity()? m->get_buffer_capacity() : std::get<1>(metric);
            requested_size = requested_size > m->get_buffer_sequence_counter()? m->get_buffer_sequence_counter() : requested_size;
            // Calculate the last element
            int last = m->get_buffer_sequence_counter() >  m->get_buffer_capacity()? m->get_buffer_capacity() : m->get_buffer_sequence_counter();
            // Get buffer type
            int buffer_type = m->get_buffer_type();

            // Run over metrics
            for (int i = 0; i<requested_size; ++i )
            {
                // If element is int, get values from the integer buffer
                if (buffer_type == TYPE_INT) 
                {
                    int_metric_buffer* ibuffer = reinterpret_cast<int_metric_buffer*>(m);
                    if(from_last) 
                        element_buffer.push_back(ibuffer->get_element(i));
                    else 
                        element_buffer.insert(element_buffer.begin(), ibuffer->get_element(last-i-1));
                }

                // If element is float, get values from the float buffer
                else if (buffer_type == TYPE_FLOAT) 
                {
                    float_metric_buffer* fbuffer = reinterpret_cast<float_metric_buffer*>(m);
                    if (from_last)
                        element_buffer.push_back(fbuffer->get_element(i));
                    else
                        element_buffer.insert(element_buffer.begin(), fbuffer->get_element(last-i-1));
                }
                
                // If element is double, get values from the double buffer
                else if (buffer_type == TYPE_DOUBLE) 
                {
                    double_metric_buffer* dbuffer = reinterpret_cast<double_metric_buffer*>(m);
                    if (from_last)
                        element_buffer.push_back(dbuffer->get_element(i));
                    else
                        element_buffer.insert(element_buffer.begin(), dbuffer->get_element(last-i-1));
                }

                // If element is bytes, get values from the bytes buffer
                else if (buffer_type == TYPE_BYTES) 
                {
                    bytes_metric_buffer* bbuffer =  reinterpret_cast<bytes_metric_buffer*>(m);
                    if (from_last)
                        element_buffer.push_back(bbuffer->get_element(i));
                    else
                        element_buffer.insert(element_buffer.begin(), bbuffer->get_element(last-i-1));
                }

                // Invalid metric type...
                else
                    throw std::runtime_error("Invalid metric type requested!");
            }

            // Append the all picked elements to the map
            metrics_map[std::get<0>(metric)] = element_buffer;
        }

        return metrics_map;
    }
    
    /**
     *  Delete all values from a last values map
     *  @param metric_last_values Metric last values got from last_values function
     */
    void delete_values_map(std::map<std::string, std::vector<element*>>& metric_last_values)
    {
        for (auto it = metric_last_values.begin(); it != metric_last_values.end(); it++) 
        {
           for (auto value : it->second){
            //If element is a byte array, trasnform it
            if (value->get_element_type() == TYPE_BYTES)
            {
                metric_element_bytes* e = reinterpret_cast<metric_element_bytes*>(value);
                delete e;
            }
            // Else, just delete the value
            else 
                delete value;
            }
        }
    }

    /**
     * Adds an integer value to a named buffer. If buffer does not exists, a new one is created with INTEGER TYPE
     * @param name Name of the buffer where the element will be added
     * @param value Integer value to add to the buffer
     */
    void add_metric(const char* name, int value) 
    {
        reinterpret_cast<int_metric_buffer*>(get_or_create_buffer(name, true, TYPE_INT))->add_element(value);
    }

    /**
     * Adds a float value to a named buffer. If buffer does not exists, a new one is created with FLOAT TYPE
     * @param name Name of the buffer where the element will be added
     * @param value Float value to add to the buffer
     */
    void add_metric(const char* name, float value) 
    {
        reinterpret_cast<float_metric_buffer*>(get_or_create_buffer(name, true, TYPE_FLOAT))->add_element(value);
    }
    
    /**
     * Adds a double value to a named buffer. If buffer does not exists, a new one is created with FLOAT TYPE
     * @param name Name of the buffer where the element will be added
     * @param value Double value to add to the buffer
     */
    void add_metric(const char* name, double value) 
    {
        reinterpret_cast<double_metric_buffer*>(get_or_create_buffer(name, true, TYPE_DOUBLE))->add_element(value);
    }

    /**
     * Adds a string value to a named buffer. If buffer does not exists, a new one is created with BYTES TYPE
     * @param name Name of the buffer where the element will be added
     * @param value String value to add to the buffer
     */
    void add_metric(const char* name, const char* value) 
    {
        reinterpret_cast<bytes_metric_buffer*>(get_or_create_buffer(name, true, TYPE_BYTES))->add_element((unsigned char*) value, std::strlen(value)+1);
    }
    
    /**
     * Adds a string value to a named buffer. If buffer does not exists, a new one is created with BYTES TYPE
     * @param name Name of the buffer where the element will be added
     * @param value String value to add to the buffer
     */
    void add_metric(const char* name, const std::string value) 
    {
        reinterpret_cast<bytes_metric_buffer*>(get_or_create_buffer(name, true, TYPE_BYTES))->add_element((unsigned char*) value.c_str(), value.size()+1);
    }
    
    
    /**
     * Adds a byte array value to a named buffer. If buffer does not exists, a new one is created with BYTES TYPE
     * @param name Name of the buffer where the element will be added
     * @param value Byte array value to add to the buffer
     * @param size Size of the element
     */
    void add_metric(const char* name, const unsigned char* value, int size) 
    {
        reinterpret_cast<bytes_metric_buffer*>(get_or_create_buffer(name, true, TYPE_BYTES))->add_element((unsigned char*) value, size);
    }

private:
    unsigned int default_capacity;
    std::map<std::string, generic_buffer*> metric_buffers;
    
    /**
     *  Get an existing named buffer or create a new one with an unique name.
     *  @param name Name of the buffer to get, or if does not exists, to create
     *  @param create_new If false, throws an exception if the named buffer does not exists, else create a new one if does not exists
     *  @param type Type of the elements of the buffer
     */
    generic_buffer* get_or_create_buffer(const char* name, bool create_new, int type) 
    {
        generic_buffer* buffer;
        
        //Find if buffer exists in the buffer map
        auto m = this->metric_buffers.find(name);
        // Metric buffer associate with this name not found
        if (m == this->metric_buffers.end()) 
        {
            //If the buffer not exists and create_new is false, throw exception
            if(!create_new)
                throw std::runtime_error("Requested metric does not exists");

            // Create a metric buffer with desired type
            if(type == TYPE_INT)
                buffer = reinterpret_cast<int_metric_buffer*>(new int_metric_buffer(this->default_capacity));
            else if (type == TYPE_FLOAT)
                buffer = reinterpret_cast<float_metric_buffer*>(new float_metric_buffer(this->default_capacity));
            else if (type == TYPE_DOUBLE)
                buffer = reinterpret_cast<double_metric_buffer*>(new double_metric_buffer(this->default_capacity));
            else if (type == TYPE_BYTES)
                buffer = reinterpret_cast<bytes_metric_buffer*>(new bytes_metric_buffer(this->default_capacity));
            else
                throw std::runtime_error("Invalid metric type");
            
            // Add the new metric buffer to the map
            this->metric_buffers[name] = buffer;
        }
        // If named buffer already exists in the map, simple return it
        else
            buffer = m->second;
            
        return buffer;
    }
}; 
}
/* C Wrapper */

#ifdef SPITS_ENTRY_POINT

extern "C" void* spits_metrics_new(int buffer_size) 
{
    return reinterpret_cast<void*>(new spits::metrics(buffer_size));
}

extern "C" void spits_set_metric_int(void* metrics, const char* metric_name, int ivalue) 
{
    reinterpret_cast<spits::metrics*>(metrics)->add_metric(metric_name, ivalue);
}

extern "C" void spits_set_metric_float(void* metrics, const char* metric_name, float fvalue) 
{
    reinterpret_cast<spits::metrics*>(metrics)->add_metric(metric_name, fvalue);
}

extern "C" void spits_set_metric_double(void* metrics, const char* metric_name, double dvalue) 
{
    reinterpret_cast<spits::metrics*>(metrics)->add_metric(metric_name, dvalue);
}

extern "C" void spits_set_metric_string(void* metrics, const char* metric_name, char* string) 
{
    reinterpret_cast<spits::metrics*>(metrics)->add_metric(metric_name, string);
}

extern "C" void spits_set_metric_bytes(void* metrics, const char* metric_name, unsigned char* byte_array, int size) 
{
     reinterpret_cast<spits::metrics*>(metrics)->add_metric(metric_name, byte_array, size);
}

extern "C" int spits_get_num_buffers(void* metrics)
{
    return reinterpret_cast<spits::metrics*>(metrics)->get_num_metric_buffers();
}

extern "C" void* spits_get_metrics_list(void* metrics) 
{
    spits::metrics* m = reinterpret_cast<spits::metrics*>(metrics);
    
    if(m->get_num_metric_buffers() == 0) return NULL;

    std::vector<std::tuple<std::string, int, int>> list = m->list();
    spits::ostream serial_stream;

    serial_stream << (int64_t) list.size();
    for (auto metric : list)
        serial_stream << std::get<0>(metric) << (int64_t) std::get<1>(metric) << (int64_t) std::get<2>(metric);

    unsigned char* metric_ptr = reinterpret_cast<unsigned char*>(std::malloc(serial_stream.pos() * sizeof(unsigned char)));
    std::memcpy(metric_ptr, serial_stream.data(), serial_stream.pos());
    return reinterpret_cast<void*>(metric_ptr);
}

std::map<std::string, std::vector<spits::element*>> _spits_metrics_last_values(
    spits::metrics* metrics, const char** metrics_list, int* sizes, bool from_last = false) 
{
    std::vector<std::tuple<std::string, int> > list;

    for (int i = 0; metrics_list[i]; ++i) 
        list.push_back({std::string(metrics_list[i]), sizes && sizes>0? sizes[i] : 1});

    return metrics->last_values(std::vector<std::tuple<std::string, int> >(list), from_last);
}

extern "C" void* spits_get_metrics_last_values(void* metrics, const char** metrics_list) 
{
    spits::ostream serial_stream;
    spits::metrics* m = reinterpret_cast<spits::metrics*>(metrics);
    if(m->get_num_metric_buffers() == 0 || !metrics_list) return NULL;

    auto metrics_map = _spits_metrics_last_values(m, metrics_list, NULL, false);
    if(metrics_map.empty()) return NULL;

    for (int i = 0; metrics_list[i]; ++i) 
    {
        std::vector<spits::element*> elements = metrics_map[std::string(metrics_list[i])];
        if (!elements.size())
        {
            m->delete_values_map(metrics_map);
            return NULL;
        }

        for (auto e : elements) 
        {
            if (e->get_element_type() == spits::TYPE_INT) 
            {
                spits::metric_element_int* ie = reinterpret_cast<spits::metric_element_int*>(e);
                serial_stream << (int64_t) ie->get_value();
            }
            else if (e->get_element_type() == spits::TYPE_FLOAT) 
            {
                spits::metric_element_float* fe = reinterpret_cast<spits::metric_element_float*>(e);
                serial_stream << (float) fe->get_value();
            }
            else if (e->get_element_type() == spits::TYPE_DOUBLE) 
            {
                spits::metric_element_double* de = reinterpret_cast<spits::metric_element_double*>(e);
                serial_stream << (double) de->get_value();
            }
            else if (e->get_element_type() == spits::TYPE_BYTES) 
            {
                spits::metric_element_bytes* be = reinterpret_cast<spits::metric_element_bytes*>(e);
                serial_stream << (int64_t) be->get_size();
                serial_stream.write_data((const void*) be->get_value(), be->get_size());
            }
            else
            {
                m->delete_values_map(metrics_map);
                return NULL;
            }
                
            serial_stream << (int64_t) e->get_element_time().tv_sec;
            serial_stream << (int64_t) e->get_element_time().tv_nsec;
            serial_stream << (int64_t) e->get_element_sequence();
        }
    }

    unsigned char* metric_ptr = reinterpret_cast<unsigned char*>(std::malloc(serial_stream.pos() * sizeof(unsigned char)));
    std::memcpy(metric_ptr, serial_stream.data(), serial_stream.pos());
    
    m->delete_values_map(metrics_map);
    return reinterpret_cast<void*>(metric_ptr);
}

extern "C" void* spits_get_metrics_history(void* metrics, const char** metrics_list, int* sizes) 
{
    spits::ostream serial_stream;
    spits::metrics* m = reinterpret_cast<spits::metrics*>(metrics);
    if(m->get_num_metric_buffers() == 0 || !metrics_list) return NULL;

    auto metrics_map = _spits_metrics_last_values(m, metrics_list, sizes, false);
    if(metrics_map.empty()) return NULL;

    for (int i = 0; metrics_list[i]; ++i) 
    {
        std::vector<spits::element*> elements = metrics_map[std::string(metrics_list[i])];
        if (!elements.size())
        {
            m->delete_values_map(metrics_map);
            return NULL;
        }

        serial_stream << (int64_t) elements[0]->get_element_sequence();
        serial_stream << (int64_t) elements.size();

        for (auto e : elements) 
        {
            if (e->get_element_type() == spits::TYPE_INT) 
            {
                spits::metric_element_int* ie = reinterpret_cast<spits::metric_element_int*>(e);
                serial_stream << (int64_t) ie->get_value();
            }
            else if (e->get_element_type() == spits::TYPE_FLOAT) 
            {
                spits::metric_element_float* fe = reinterpret_cast<spits::metric_element_float*>(e);
                serial_stream << (float) fe->get_value();
            }
            else if (e->get_element_type() == spits::TYPE_DOUBLE) 
            {
                spits::metric_element_double* de = reinterpret_cast<spits::metric_element_double*>(e);
                serial_stream << (double) de->get_value();
            }
            else if (e->get_element_type() == spits::TYPE_BYTES) 
            {
                spits::metric_element_bytes* be = reinterpret_cast<spits::metric_element_bytes*>(e);
                serial_stream << (int64_t) be->get_size();
                serial_stream.write_data((const void*) be->get_value(), be->get_size());
            }
            else
            {
                m->delete_values_map(metrics_map);
                return NULL;
            }
                
            serial_stream << (int64_t) e->get_element_time().tv_sec;
            serial_stream << (int64_t) e->get_element_time().tv_nsec;
        }
    }
    
    unsigned char* metric_ptr = reinterpret_cast<unsigned char*>(std::malloc(serial_stream.pos() * sizeof(unsigned char)));
    std::memcpy(metric_ptr, serial_stream.data(), serial_stream.pos());
    
    m->delete_values_map(metrics_map);
    return reinterpret_cast<void*>(metric_ptr);
}

extern "C" void spits_free_ptr(void* p) 
{
    if(p) std::free(p);
}

extern "C" void spits_metrics_reset(void* metrics)
{
    reinterpret_cast<spits::metrics*>(metrics)->reset();
}

extern "C" void spits_metrics_delete(void* metrics) 
{
    delete reinterpret_cast<spits::metrics*>(metrics);
}

extern "C" void spits_metrics_debug_dump(void* metrics)
{
    spits::metrics* m = reinterpret_cast<spits::metrics*>(metrics);
    std::vector<std::tuple<std::string, int> > m_list;

    for (auto e : m->list())
        m_list.push_back({std::get<0>(e), 1});
        
    auto m_values = m->last_values(m_list);
        
    for (auto it = m_values.begin(); it != m_values.end(); it++){
        std::cout << it->first << ": ";
        if (it->second[0]->get_element_type() == spits::TYPE_INT)
            std::cout << reinterpret_cast<spits::metric_element_int*>(it->second[0])->get_value() << std::endl;
            
        else if (it->second[0]->get_element_type() == spits::TYPE_FLOAT)
            std::cout << reinterpret_cast<spits::metric_element_float*>(it->second[0])->get_value() << std::endl;
            
        else if (it->second[0]->get_element_type() == spits::TYPE_DOUBLE)
            std::cout << reinterpret_cast<spits::metric_element_double*>(it->second[0])->get_value() << std::endl;
            
        else if (it->second[0]->get_element_type() == spits::TYPE_BYTES)
            std::cout << reinterpret_cast<spits::metric_element_bytes*>(it->second[0])->get_value() << std::endl;
        else
            std::cout << "Invalid type" << std::endl;
    }
    
    m->delete_values_map(m_values);
}


#endif /* SPITS_ENTRY_POINT */

#endif /* __METRICS_HPP__ */

