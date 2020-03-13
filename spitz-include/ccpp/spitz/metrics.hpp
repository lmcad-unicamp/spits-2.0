#ifndef __METRICS_H_
#define __METRICS_H_


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
#include "stream.hpp"


enum TYPE {
    TYPE_INT = 0,
    TYPE_FLOAT,
    TYPE_BYTES
};

template <typename T>
class Ring {
public:
    explicit Ring(int _capacity = 1) : m_data(new T[_capacity]()), _head(0),
        _tail(0), _capacity(_capacity), _size(0)
    {}

    ~Ring()
    {
        while(!empty()) pop();
        delete[] m_data;
    }

    void push(T param)
    {
        if (full()) pop();
        m_data[_head] = std::forward<T>(param);
        _head = (_head+1)%_capacity;
        _size++;
    }

    T pop_front(void)
    {
        if (empty()) return nullptr;
        _head = (_head-1)%_capacity;
        if(_head < 0)
            _head = _capacity-1;
        auto m_ptr = std::forward<T>(m_data[_head]);
        _size--;
        return m_ptr;
    }

    T pop(void)
    {
        if (empty()) return nullptr;
        auto m_ptr = std::forward<T>(m_data[_tail]);
        _tail = (_tail+1)%_capacity;
        _size--;
        return m_ptr;
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

private:
    T empty_value;
    T* m_data;
    int _head, _tail, _capacity;
    int _size;
};


struct BaseMetricElement {
    struct timespec timestamp;
    unsigned int seq_no;

    BaseMetricElement(unsigned int sequence = 0) : seq_no(sequence) {
        clock_gettime(CLOCK_REALTIME, &timestamp);
    }

    virtual ~BaseMetricElement() {} ;

    virtual std::string to_string() = 0;

    virtual int get_type() = 0;
};

template <typename T, int U>
struct Element : public BaseMetricElement {
    T val;
    Element(T value, unsigned int sequence = 0) :
        BaseMetricElement(sequence), val(value) { }

    virtual ~Element() { };

    T operator* (void) {
        return val;
    }

    virtual int get_type() {
        return U;
    }

    virtual std::string to_string(void) {
        std::stringstream stream;
        stream << timestamp.tv_sec << ":" << timestamp.tv_nsec << " " << seq_no << " " << val;
        return std::string(stream.str());
    }

};


struct BytesElement : public Element<unsigned char*, TYPE_BYTES> {

    BytesElement(unsigned char* ptr, int size, unsigned int sequence = 0) : Element(ptr, sequence), size(size) {
    }

    virtual ~BytesElement() {
        if(val)
            delete[] val;
    };

    unsigned char* operator* (void) {
        return val;
    }

    virtual std::string to_string(void) {
        std::stringstream stream;
        stream << timestamp.tv_sec << ":" << timestamp.tv_nsec << " " << seq_no << " " << val;
        return std::string(stream.str());
    }

private:
    int size;
};

typedef struct Element<int, TYPE_INT> IntElement;
typedef struct Element<float, TYPE_FLOAT> FloatElement;
typedef struct BytesElement BytesElement;

typedef Ring<std::unique_ptr<BaseMetricElement>> GenericRing;
typedef Ring<std::unique_ptr<IntElement>> IntRing;
typedef Ring<std::unique_ptr<FloatElement>> FloatRing;
typedef Ring<std::unique_ptr<BytesElement>> BytesRing;


class BaseMetricBuffer {
public:
    BaseMetricBuffer(int type, int seq_no = 0, int size=1) : type(type), sequence(seq_no), size(size) {
    };

    virtual ~BaseMetricBuffer() { }

    virtual int get_type(void) {
        return type;
    }

    virtual int get_size(void) {
        return size;
    }

    virtual int get_sequence(void) {
        return sequence;
    }

    virtual int length() = 0;

protected:
    int sequence;
    int type;
    int size;
};

template <typename T, typename U, int enumtype>
class MetricBuffer : public BaseMetricBuffer {
    using primitive_type = U;
    using metric_element_type = T;
    using metric_element_ptr = std::unique_ptr<metric_element_type>;
    using ring_type = Ring<metric_element_ptr>;
    ring_type* buffer;

public:
    MetricBuffer(int size) : BaseMetricBuffer(enumtype, 0, size),
        buffer(new ring_type(size)) { }

    ~MetricBuffer() {
        if (buffer)
            delete buffer;
    }


    virtual void add_element(primitive_type element) {
        buffer->push(std::move(metric_element_ptr(
            new metric_element_type(element, sequence++))));
    }

    virtual metric_element_type* get_element(void) {
        auto ptr = std::move(buffer->pop());
        T* element = ptr.get();
        ptr.release();
        return element;
    }

    virtual metric_element_type* get_front_element(void) {
        auto ptr = std::move(buffer->pop_front());
        T* element = ptr.get();
        ptr.release();
        return element;
    }

    virtual int length(void) {
        return buffer->length();
    }
};

typedef MetricBuffer<IntElement, int, TYPE_INT> IntBuffer;
typedef MetricBuffer<FloatElement, float, TYPE_FLOAT> FloatBuffer;
typedef MetricBuffer<BytesElement, unsigned char*, TYPE_BYTES> BytesBuffer;

class Metrics {
    int default_size;
    std::map<std::string, BaseMetricBuffer*> metrics;

public:
    Metrics(int size) : default_size(size) {
    }

    ~Metrics() {
        for (auto it = metrics.begin(); it != metrics.end(); it++) {
            if (it->second)
                delete it->second;
        }

        metrics.clear();
    }

    std::vector<std::tuple<std::string, int, int>> metrics_list(void) {
        std::vector<std::tuple<std::string, int, int>> list;
        for (auto const& key : metrics)
            list.push_back({key.first, (uint64_t) key.second->get_size(), key.second->get_type()});

        return list;
    }

    std::map<std::string, std::vector<BaseMetricElement*>> metrics_last_values(std::vector<std::tuple<std::string, int>> metrics_list, bool lasts = true) {
        std::map<std::string, std::vector<BaseMetricElement*>> metrics_map;


        for(auto const& metric : metrics_list) {
            std::vector<BaseMetricElement*> element_buffer;
            BaseMetricElement* element;
            auto M = metrics.find(std::get<0>(metric));
            if (M == metrics.end()) {
                metrics_map.clear();
                break;
            }

            auto m = M->second;

            int requested_size = std::get<1>(metric) > m->get_size()? m->get_size() : std::get<1>(metric);
            requested_size = requested_size > m->length()? m->length() : requested_size;
            int buffer_type = m->get_type();

            for (int i = 0; i<requested_size; ++i ){
                if (buffer_type == TYPE_INT) {
                    IntBuffer* ibuffer = reinterpret_cast<IntBuffer*>(m);
                    element = lasts? ibuffer->get_front_element() : ibuffer->get_element();
                    element_buffer.push_back(element);
                }

                else if (buffer_type == TYPE_FLOAT) {
                    FloatBuffer* fbuffer = reinterpret_cast<FloatBuffer*>(m);
                    element_buffer.push_back(lasts? fbuffer->get_front_element() : fbuffer->get_element());
                }

                else if (buffer_type == TYPE_BYTES) {
                    BytesBuffer* bbuffer =  reinterpret_cast<BytesBuffer*>(m);
                    element_buffer.push_back(lasts? bbuffer->get_front_element() : bbuffer->get_element());
                }

                else
                    throw std::runtime_error("Invalid metric type requested!");
            }

            metrics_map[std::get<0>(metric)] = element_buffer;
        }

        return metrics_map;
    }

    // std::vector<std::tuple<std::string, >> metrics_last_values(void) {
    //     std::vector<std::string> list;
    //     for (auto it = metrics.begin(); it != metrics.end(); it++)
    //         list.push_back(it->first);
    //
    //     return list;
    // }

    void add_metric(const char* name, int value) {
        IntBuffer* buffer = reinterpret_cast<IntBuffer*>(get_or_create_buffer(name, true, TYPE_INT));
        buffer->add_element(value);
    }

    void add_metric(const char* name, float value) {
        FloatBuffer* buffer = reinterpret_cast<FloatBuffer*>(get_or_create_buffer(name, true, TYPE_FLOAT));
        buffer->add_element(value);
    }

    void add_metric(const char* name, unsigned char* value) {
        BytesBuffer* buffer = reinterpret_cast<BytesBuffer*>(get_or_create_buffer(name, true, TYPE_BYTES));
        buffer->add_element(value);
    }

    std::tuple<BaseMetricElement*, int> get_metric(const char* name) {
        BaseMetricElement* element;
        auto buffer = get_or_create_buffer(name, false);
        int type = buffer->get_type();

        if (type == TYPE_INT)
            return {(reinterpret_cast<IntBuffer*>(buffer))->get_element(), TYPE_INT};
        else if (type == TYPE_FLOAT)
            return {(reinterpret_cast<FloatBuffer*>(buffer))->get_element(), TYPE_FLOAT};
        else if (type == TYPE_BYTES)
            return {(reinterpret_cast<BytesBuffer*>(buffer))->get_element(), TYPE_BYTES};
        else
            throw std::runtime_error("Invalid metric type");
    }

private:
    BaseMetricBuffer* get_or_create_buffer(const char* name, bool exists_ok, int type=TYPE_INT) {
        BaseMetricBuffer* buffer;
        auto m = metrics.find(name);

        if (m == metrics.end()) {
            if(!exists_ok)
                throw std::runtime_error("Requested metric does not exists!");

            if(type == TYPE_INT)
                buffer = reinterpret_cast<IntBuffer*>(new IntBuffer(default_size));
            else if (type == TYPE_FLOAT)
                buffer = reinterpret_cast<FloatBuffer*>(new FloatBuffer(default_size));
            else if (type == TYPE_BYTES)
                buffer = reinterpret_cast<BytesBuffer*>(new BytesBuffer(default_size));
            else
                throw std::runtime_error("Invalid metric type!");
            metrics[name] = buffer;
        }
        else
            buffer = m->second;

        return buffer;
    }
};


#if 1
//#ifdef METRICS

#ifndef DEFAULT_BUFFER_SIZE
#define DEFAULT_BUFFER_SIZE 10
#endif
/* C Wrapper */
Metrics* spits_metrics = new Metrics(DEFAULT_BUFFER_SIZE);

extern "C" void spits_metrics_new(int buffer_size) {
    //if (SpitsMetric::get_metric())
    //    SpitsMetric::metrics = new Metrics(buffer_size);
    if (spits_metrics)
        delete spits_metrics;
    spits_metrics = new Metrics(buffer_size);
}

extern "C" void spits_set_metric_int(const char* metric_name, int ivalue) {
    if (spits_metrics)
        spits_metrics->add_metric(metric_name, ivalue);
}

extern "C" void spits_set_metric_float(const char* metric_name, float fvalue) {
    if (spits_metrics)
        spits_metrics->add_metric(metric_name, fvalue);
}

extern "C" void spits_set_metric_bytes(const char* metric_name, unsigned char* byte_array) {
    if (spits_metrics)
        spits_metrics->add_metric(metric_name, byte_array);
}

extern "C" void* spits_get_metrics_list(void) {
    if (!spits_metrics)
        return NULL;

    std::vector<std::tuple<std::string, int, int>> list = spits_metrics->metrics_list();
    spitz::ostream serial_stream;

    serial_stream << (int64_t) list.size();
    for (auto metric : list)
        serial_stream << std::get<0>(metric) << (int64_t) std::get<1>(metric) << (int64_t) std::get<2>(metric);

    unsigned char* metric_ptr = reinterpret_cast<unsigned char*>(std::malloc(serial_stream.pos() * sizeof(unsigned char)));
    std::memcpy(metric_ptr, serial_stream.data(), serial_stream.pos());
    return reinterpret_cast<void*>(metric_ptr);
}

std::map<std::string, std::vector<BaseMetricElement*>> _spits_metrics_last_values(const char** metrics_list, int* sizes, bool lasts = true) {
    std::vector<std::tuple<std::string, int>> list;

    for (int i = 0; metrics_list[i]; ++i) {
        list.push_back({std::string(metrics_list[i]), sizes? sizes[i] : 1});
    }

    return spits_metrics->metrics_last_values(std::vector<std::tuple<std::string, int>>(list), lasts);
}

extern "C" void* spits_get_metrics_last_values(const char** metrics_list) {
    if (!spits_metrics)
        return NULL;

    auto metrics_map = _spits_metrics_last_values(metrics_list, NULL, true);
    if(metrics_map.empty())
        return NULL;

    // std::map<std::string, std::vector<BaseMetricElement*>> metrics_last_values
    spitz::ostream serial_stream;
    for (int i = 0; metrics_list[i]; ++i) {
        std::vector<BaseMetricElement*> elements = metrics_map[std::string(metrics_list[i])];
        if (!elements.size())
            return NULL;

        for (auto element : elements) {
            if (element->get_type() == TYPE_INT) {
                IntElement* e = reinterpret_cast<IntElement*>(element);
                serial_stream << (int64_t) e->val;
                serial_stream << (int64_t) element->timestamp.tv_sec << (int64_t) element->timestamp.tv_nsec << (int64_t) element->seq_no;
                delete e;
            }
            else if (element->get_type() == TYPE_FLOAT) {
                FloatElement* e = reinterpret_cast<FloatElement*>(element);
                serial_stream << (float) e->val;
                serial_stream << (int64_t) element->timestamp.tv_sec << (int64_t) element->timestamp.tv_nsec << (int64_t) element->seq_no;
                delete e;
            }
            else if (element->get_type() == TYPE_BYTES) {
                BytesElement* e = reinterpret_cast<BytesElement*>(element);
                serial_stream << std::string((const char*)e->val);
                serial_stream << (int64_t) element->timestamp.tv_sec << (int64_t) element->timestamp.tv_nsec << (int64_t) element->seq_no;
                delete e;
            }
            else
                return NULL;
        }
    }

    unsigned char* metric_ptr = reinterpret_cast<unsigned char*>(std::malloc(serial_stream.pos() * sizeof(unsigned char)));
    std::memcpy(metric_ptr, serial_stream.data(), serial_stream.pos());
    return reinterpret_cast<void*>(metric_ptr);
}

extern "C" void* spits_get_metrics_history(const char** metrics_list, int* sizes) {
    if (!spits_metrics)
        return NULL;

    auto metrics_map = _spits_metrics_last_values(metrics_list, sizes, false);
    if(metrics_map.empty())
        return NULL;

    spitz::ostream serial_stream;
    for (int i = 0; metrics_list[i]; ++i) {
        std::vector<BaseMetricElement*> elements = metrics_map[std::string(metrics_list[i])];
        if (!elements.size())
            return NULL;

        serial_stream << (int64_t) elements[0]->seq_no << (int64_t) elements.size();

        for (auto element : elements) {
            if (element->get_type() == TYPE_INT) {
                IntElement* e = reinterpret_cast<IntElement*>(element);
                serial_stream << (int64_t) e->val;
                serial_stream << (int64_t) element->timestamp.tv_sec << (int64_t) element->timestamp.tv_nsec;
                delete e;
            }
            else if (element->get_type() == TYPE_FLOAT) {
                FloatElement* e = reinterpret_cast<FloatElement*>(element);
                serial_stream << (float) e->val;
                serial_stream << (int64_t) element->timestamp.tv_sec << (int64_t) element->timestamp.tv_nsec;
                delete e;
            }
            else if (element->get_type() == TYPE_BYTES) {
                BytesElement* e = reinterpret_cast<BytesElement*>(element);
                serial_stream << std::string((const char*)e->val);
                serial_stream << (int64_t) element->timestamp.tv_sec << (int64_t) element->timestamp.tv_nsec;
            }
            else
                return NULL;
        }
    }

    unsigned char* metric_ptr = reinterpret_cast<unsigned char*>(std::malloc(serial_stream.pos() * sizeof(unsigned char)));
    std::memcpy(metric_ptr, serial_stream.data(), serial_stream.pos());
    return reinterpret_cast<void*>(metric_ptr);
}


extern "C" void spits_free_ptr(void* p) {
    if(p)
        std::free(p);
}

extern "C" void spits_metrics_delete(void) {
    //spitz::metrics* metrics = reinterpret_cast<spitz::metrics*>(metrics_data);
    if(spits_metrics)
        delete spits_metrics;
}

#endif

#endif
