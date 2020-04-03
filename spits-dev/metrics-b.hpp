#ifndef __METRICS_H_
#define __METRICS_H_

#include <cstring>
#include <ctime>
#include <cstdint>
#include <map>
#include <iostream>
#include "stream.hpp"

template <typename T> class DestroyableElement {
    T m_data;
public:
    DestroyableElement() {};
    DestroyableElement(T& data) : m_data(data) { };
    ~DestroyableElement(void) { m_data.~T(); };
    inline T& data(void) const { return m_data; }
    DestroyableElement<T>& operator=(T& value) {
        m_data.~T(); m_data = value;
        return *this;
    };
};

template <typename T> class DestroyableElement<T*> {
    T* m_data;
public:
    DestroyableElement(void) : m_data(nullptr) {};
    DestroyableElement(T& data) : m_data(data) {};
    ~DestroyableElement(void) { if(m_data) delete(m_data); };
    T* data(void) const { return m_data; }
    DestroyableElement<T*>& operator=(T* value) {
        if(m_data) delete m_data;
        m_data = value;
        return *this;
    };
};

template <typename Type> struct BaseMetricElement {
    Type value;
    struct timespec timestamp;
    uint64_t sequence_no;

    BaseMetricElement(void) {}
    BaseMetricElement(Type val, struct timespec& time, uint64_t sequence) : value(val), sequence_no(sequence) {
        std::memcpy((void*) &this->timestamp, (void*) &time, sizeof(struct timespec));
    }

    virtual ~BaseMetricElement(void) {}
};

struct MetricElement_Int64 : public BaseMetricElement<int64_t> {
    MetricElement_Int64(int64_t data, struct timespec& time, uint64_t sequence) : BaseMetricElement(data, time, sequence) {}
    ~MetricElement_Int64(){};
};

struct MetricElement_Float : public BaseMetricElement<float> {
    MetricElement_Float(float data, struct timespec& time, uint64_t sequence) : BaseMetricElement(data, time, sequence) {}
};

struct MetricElement_Bytes : public BaseMetricElement<int8_t*> {
    unsigned int size;

    MetricElement_Bytes(int8_t* data, struct timespec& time, uint64_t sequence, unsigned int size) : BaseMetricElement(data, time, sequence), size(size) {}
    ~MetricElement_Bytes() { if(value) delete value; }
};

struct MetricElement_Time : public BaseMetricElement<struct timespec> {
    MetricElement_Time(struct timespec& data, struct timespec& time, uint64_t sequence){
        std::memcpy((void*) &this->value, (void*) &data, sizeof(struct timespec));
        std::memcpy((void*) &this->timestamp, (void*) &time, sizeof(struct timespec));
        this->sequence_no = sequence;
    }
};


template <typename BufferType> class RingBuffer {
    unsigned int _head, _tail, _buffer_size;
    bool _full;
    DestroyableElement<BufferType>* _buffer;

public:
    /*!
     * Create a circular buffer
     * @param size Size of the buffer
     * @param
     */
    RingBuffer(unsigned int size=1) :
        _buffer_size(size), _head (0), _tail(0), _full(false), _buffer(new DestroyableElement<BufferType>[size]){}

    ~RingBuffer(void) { delete[] _buffer; }

    /*!
     * Reset the buffer
     */
    void reset(void) {
        delete[] _buffer;
        _buffer = new DestroyableElement<BufferType>[_buffer_size];
        _head = _tail = _full = 0;
    }

    /*
     * Sets the number of elements to 0. Doesn't delete the elements
     */
    void clear(void) {
        _head = _tail = _full = 0;
    }

    /*!
    * Check if the buffer is full
    * @return True if buffer is buffer, false otherwise
    */
    inline bool full(void) const { return _full; }

    /*!
    * Check if the buffer is empty
    * @return True if buffer is buffer, false otherwise
    */
    inline bool empty(void) const { return (!_full && _head == _tail); }

    /*!
     * Number of elements inserted in the Circular Buffer
     * @return  Returns the number of elements in the buffer
     */
    unsigned int length(void) const {
        if(!full()) {
            if(_head >= _tail)
                return _head - _tail;
            else
                return _buffer_size+_head-_tail;
        }

        else
            return _buffer_size;
    }

    /*!
     * The number of elements the Circular Buffer can holds
     * @return Returns the number of elements the Circular Buffer can holds
     */
    unsigned int capacity(void) const { return _buffer_size; }

    /*!
     * Inserts an element in the circular buffer
     * @param  element  Element to be inserted
     */
    void push(BufferType element) {
        _buffer[_head] = element;
        if(full()) _tail = (_tail + 1) % _buffer_size;
        _head = (_head + 1) % _buffer_size;
        _full = (_head == _tail);
    }

    /*!
     * Removes the oldest element from the circular buffer
     * @return  The removed element from the buffer
     */
    BufferType pop(void) {
        if(empty()) throw std::runtime_error("Trying to pop, but the buffer is empty!");
        auto& val = _buffer[_tail];
        _full = false;
        _tail = (_tail + 1) % _buffer_size;
        return val.data();
    }

    /*!
     * Peeks the oldest element in the buffer
     * @return  The last element
     */
    BufferType peek(void) const {
        return _buffer[_tail].data();
    }

    /*!
     * Peeks the newest element in the buffer
     * @return  Returns the first element
     */
    BufferType peek_front(void) const {
        return _buffer[(_head-1)%_buffer_size].data();
    }

    /*!
     * Pushs an element to the buffer
     * @param element Element to be inserted
     */
    void operator+=(const BufferType& element) {
        push(element);
    }

    BufferType at(unsigned int index) {
        return _buffer[(_head+index)%_buffer_size].data();
    }

    BufferType operator[](unsigned int index) {
        return _buffer[(_head+index)%_buffer_size].data();
    }

/*public:
    template <typename T> class _iterator {
        friend class RingBuffer;
    public:
        _iterator (RingBuffer& cb, unsigned int head, unsigned int size) : _cb(cb), _head(head), _size(size) {}

        T operator* (void) { return _cb._buffer[_head].data(); }
        T operator& (void) { return &_cb._buffer[_head].data(); }
        _iterator& operator++ (void) { _head = (_head+1)%_cb._buffer_size; _size++; return *this; }
        _iterator& operator-- (void) { _head = (_head-1)%_cb._buffer_size; _size--; return *this; }
        bool operator!= (const _iterator& it) { return _size != it._size; }
        bool operator== (const _iterator& it) { return _size == it._size; }
    private:
        RingBuffer& _cb;
        unsigned int _head;
        unsigned int _size;
    };

    typedef _iterator<BufferType> iterator;
    _iterator<BufferType> begin() { return _iterator<BufferType>(*this, _tail, 0); }
    _iterator<BufferType> end() { return _iterator<BufferType>(*this, _head, size()); }
    _iterator<BufferType> at(int index) { if(index < 0) index += size(); return _iterator<BufferType>(*this, (_tail+index)%_buffer_size, index); } */
};

namespace spitz {
    enum metric_type {
        TYPE_INT = 0,
        TYPE_FLOAT,
        TYPE_BYTE_ARRAY,
        TYPE_DATE_TIME
    };

    class BaseBuffer {
    protected:
        enum metric_type type;
        unsigned int size;
        uint64_t sequence_no;

    public:
        BaseBuffer(enum metric_type type, unsigned int size, uint64_t sequence) : type(type), size(size), sequence_no(sequence) {}
        virtual ~BaseBuffer() { }

        unsigned int get_size(void) { return size; }
        uint64_t get_sequence_no(void) { return sequence_no; }
        enum metric_type get_type(void) { return type; }
    };

    template <typename MetricElementType> class MetricBuffer : public BaseBuffer {
        RingBuffer<MetricElementType> *buffer;

    public:
        MetricBuffer(unsigned int buffer_size=1, enum metric_type type = TYPE_INT) : BaseBuffer(type, buffer_size, 0), buffer(new RingBuffer<MetricElementType>(buffer_size)) {}
        ~MetricBuffer() { delete buffer; }

        void set_metric(MetricElementType value) { buffer->push(value); sequence_no++; }
        MetricElementType get_metric(unsigned int index) { return buffer->at(index); }
        MetricElementType get_last_metric(void) { return buffer->peek_front(); }
        MetricElementType pop_metric(void) { return buffer->pop(); }
        void clear(void) { buffer->clear(); }

        unsigned int capacity(void) { return size; }
        unsigned int length(void) { return buffer->length(); }
        unsigned int get_sequence_no(void) { return sequence_no; }
    };

    class metrics {
        unsigned int default_size;
        std::map<std::string, BaseBuffer*> buffers;
        spitz::ostream serial_stream;

        BaseBuffer* find_buffer(const char* metric_name) {
            auto map_it = buffers.find(metric_name);
            if(map_it == buffers.end()) return NULL;
            return map_it->second;
        }

        BaseBuffer* create_buffer(const char* metric_name, unsigned int size, enum metric_type type = TYPE_INT) {
            BaseBuffer* b;
            switch(type) {
                case TYPE_INT:
                    b = new MetricBuffer<MetricElement_Int64*>(size, type);
                    break;
                case TYPE_FLOAT:
                    b = new MetricBuffer<MetricElement_Float*>(size, type);
                    break;
                case TYPE_DATE_TIME:
                    b = new MetricBuffer<MetricElement_Time*>(size, type);
                    break;
                case TYPE_BYTE_ARRAY:
                    b = new MetricBuffer<MetricElement_Bytes*>(size, type);
                    break;
                default:
                    throw std::runtime_error("Invalid metric type\n");
            }
            buffers[metric_name] = b;
            return b;
        }

        std::string metric_type_name(metric_type type) {
            switch (type) {
                case TYPE_INT:
                    return "INTEGER_64";
                case TYPE_FLOAT:
                    return "FLOAT";
                case TYPE_DATE_TIME:
                    return "DATE";
                case TYPE_BYTE_ARRAY:
                    return "BYTE_ARRAY";
                default:
                    throw std::runtime_error("Error, setting a metric of an invalid type 3!");
                    break;
            }
        }

    public:
        metrics(unsigned int size=1) : default_size(size) {}

        ~metrics() {
            for (auto const& key : buffers) {
                auto k = reinterpret_cast<MetricBuffer<MetricElement_Int64*>*>(key.second);
                delete(k);
            }
            buffers.clear();
        }

        void set_metric(const char* metric_name, void* data, enum metric_type type = TYPE_INT) {
            struct timespec now;
            clock_gettime(CLOCK_REALTIME, &now);

            BaseBuffer* buffer = find_buffer(metric_name);
            if(!buffer) buffer = create_buffer(metric_name, default_size, type);

            switch(type) {
                case TYPE_INT: {
                    auto metric_b = reinterpret_cast<MetricBuffer<MetricElement_Int64*>*>(buffer);
                    metric_b->set_metric(
                        new MetricElement_Int64(*(int64_t*) data, now, metric_b->get_sequence_no()));
                    break;
                }
                case TYPE_FLOAT: {
                    auto metric_b = reinterpret_cast<MetricBuffer<MetricElement_Float*>*>(buffer);
                    metric_b->set_metric(
                        new MetricElement_Float(*(float*) data, now, metric_b->get_sequence_no()));
                    break;
                }
                case TYPE_DATE_TIME: {
                    auto metric_b = reinterpret_cast<MetricBuffer<MetricElement_Time*>*>(buffer);
                    metric_b->set_metric(
                        new MetricElement_Time(*(struct timespec*) data, now, metric_b->get_sequence_no()));
                    break;
                }
                //case TYPE_BYTE_ARRAY:
                //    return new MetricBuffer<MetricElement_Bytes>(type, size);
                default:
                    throw std::runtime_error("Invalid metric type\n");
            }
        }

        void set_metric_array(const char* metric_name, void* data, unsigned int size, enum metric_type = TYPE_INT) {
        }

        void* get_metrics_list(void) {
            serial_stream.clear();
            serial_stream <<  (int64_t) buffers.size();
            for (auto const& key : buffers)
                serial_stream << key.first << (uint64_t) key.second->get_size() << metric_type_name(key.second->get_type());

            return reinterpret_cast<void*>((char*) serial_stream.data());
        }

        void* get_metrics_last_values(std::vector<std::string>& metrics_list) {
            serial_stream.clear();

            for(std::string& metric : metrics_list) {
                BaseBuffer* buffer = find_buffer(metric.c_str());
                if(!buffer) {
                    std::stringstream error_msg;
                    error_msg << "Requested metric: \'" << metric << "\' does not exists!";
                    throw std::runtime_error(error_msg.str());
                }

                switch(buffer->get_type()) {
                    case TYPE_INT: {
                        auto metric_b = reinterpret_cast<MetricBuffer<MetricElement_Int64*>*>(buffer);
                        auto element = metric_b->get_last_metric();
                        serial_stream << (int64_t) element->value;
                        serial_stream << (int64_t) element->timestamp.tv_sec << (int64_t) element->timestamp.tv_nsec << (int64_t) element->sequence_no;
                        break;
                    }
                    case TYPE_FLOAT: {
                        auto metric_b = reinterpret_cast<MetricBuffer<MetricElement_Float*>*>(buffer);
                        auto element = metric_b->get_last_metric();
                        serial_stream << (float) element->value;
                        serial_stream << (int64_t) element->timestamp.tv_sec << (int64_t) element->timestamp.tv_nsec << (int64_t) element->sequence_no;
                        break;
                    }
                    case TYPE_DATE_TIME: {
                        auto metric_b = reinterpret_cast<MetricBuffer<MetricElement_Time*>*>(buffer);
                        auto element = metric_b->get_last_metric();
                        serial_stream << (int64_t) element->value.tv_sec << (int64_t) element->value.tv_nsec;
                        serial_stream << (int64_t) element->timestamp.tv_sec << (int64_t) element->timestamp.tv_nsec << (int64_t) element->sequence_no;
                        break;
                    }

                    default:
                        throw std::runtime_error("Invalid metric type\n");
                }
            }

            return reinterpret_cast<void*>((char*) serial_stream.data());
        }

        void* get_metrics_history(std::vector<std::string>& metrics_list, std::vector<int>& history_sizes) {
            serial_stream.clear();
            int i = 0;

            for(std::string& metric : metrics_list) {
                int actual_size = history_sizes[i++];
                BaseBuffer* buffer = find_buffer(metric.c_str());
                if(!buffer) throw std::runtime_error("Requested metric doesn't exists!");

                switch(buffer->get_type()) {
                    case TYPE_INT: {
                        auto metric_b = reinterpret_cast<MetricBuffer<MetricElement_Int64*>*>(buffer);
                        int64_t len = metric_b->length();
                        if(len-actual_size < 0) actual_size = len;
                        auto element = metric_b->get_metric(len-actual_size);
                        serial_stream << (int64_t) element->sequence_no << (int64_t) actual_size;

                        for (int j=0; j<actual_size; ++j) {
                            element = metric_b->get_metric(len-actual_size+j);
                            serial_stream << (int64_t) element->value;
                            serial_stream << (int64_t) element->timestamp.tv_sec << (int64_t) element->timestamp.tv_nsec;
                        }

                        break;
                    }

                    default:
                        throw std::runtime_error("Invalid metric type\n");
                }
            }

            return reinterpret_cast<void*>((char*) serial_stream.data());
        }
    };

};

extern spitz::metrics* metrics;

/* C Wrapper */
extern "C" void* spits_metric_new(int buffer_size) {
    if (!metrics)
        metrics = new spitz::metrics(buffer_size);
    return reinterpret_cast<void*>(metrics);
}

extern "C" void spits_metric_finish(void) {
    //spitz::metrics* metrics = reinterpret_cast<spitz::metrics*>(metrics_data);
    if(metrics) delete metrics;
}

extern "C" void* spits_get_metrics_list(void) {
    if(metrics) return metrics->get_metrics_list();
    return NULL;
}

extern "C" void* spits_get_metrics_last_values(const char** metrics_list) {
    std::vector<std::string> list;
    for (int i = 0; metrics_list[i]; ++i) {
        std::string s(metrics_list[i]);
        list.push_back(s);
    }

    if (metrics) return metrics->get_metrics_last_values(list);
    return NULL;
}

extern "C" void* spits_get_metrics_history(const char** metrics_list, int* n_list) {
    std::vector<std::string> list;
    std::vector<int> sizes;
    int i=0;
    while(metrics_list[i]) {
        std::string s(metrics_list[i]);
        list.push_back(s);
        sizes.push_back(n_list[i]);
        i++;
    }
    if (metrics) return metrics->get_metrics_history(list, sizes);
    return NULL;
}

//Only for Integer, must define for types...
extern "C" void spits_set_metric(const char* metric_name, void* value) {
    if(metrics)
        metrics->set_metric(metric_name, value);
}

extern "C" void spits_set_metric_int(const char* metric_name, int value) {
    if(metrics){
        int64_t* i = new int64_t();
        *i = value;
        metrics->set_metric(metric_name, (void*) i);
        delete i;
    }
}
#endif  /*__METRICS_H_ */
