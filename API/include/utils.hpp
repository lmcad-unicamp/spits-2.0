#ifndef __UTILS_H__
#define __UTILS_H__

#include <iostream>
#include <cstdint>
#include <map>
#include <vector>
#include <memory>
#include <exception>
#include <cstring>
#include <ctime>
#include "stream.hpp"

class BaseBuffer {
public:
    virtual ~BaseBuffer() { }
};

template <typename BufferType>
class CircularBuffer : public BaseBuffer {

public:
    /*!
     * Create a circular buffer
     * @param size Size of the buffer
     * @param
     */
    CircularBuffer(unsigned int size=1) :
        buffer_size(size), head (0), tail(0), full(false), buffer(std::unique_ptr<BufferType[]>(new BufferType[size])){
    }

    CircularBuffer(CircularBuffer<BufferType>& other) {
        buffer_size = other.buffer_size;
        head = other.head;
        tail = other.tail;
        full = other.full;
        buffer = new BufferType[buffer_size];

        for (int i=0; i<buffer_size; ++i)
            buffer[i] = other.buffer[i];

    }

    ~CircularBuffer(void) {
        delete[] buffer;
    }
private:
    /*!
     * Advance the buffer position
     */
    inline void advance(void) {
        if(is_full())
            tail = (tail + 1) % buffer_size;

        head = (head + 1) % buffer_size;
        full = (head == tail);
    }

    /*!
     * Retreat the buffer position
     */
    inline void retreat(void) {
        full = false;
        tail = (tail + 1) % buffer_size;
    }

    unsigned int buffer_size;
    unsigned int head;
    unsigned int tail;
    bool full;
    BufferType* buffer;
public:

    /*!
     * Reset the buffer
     */
    void reset(void) {
        head = tail = full = 0;
    }

    /*!
    * Check if the buffer is full
    * @return True if buffer is buffer, false otherwise
    */
    inline bool full(void) const {
        return full;
    }

    /*!
    * Check if the buffer is empty
    * @return True if buffer is buffer, false otherwise
    */
    inline bool empty(void) const {
        return (!full && head == tail);
    }

    /*!
     * Number of elements inserted in the Circular Buffer
     * @return  Returns the number of elements in the buffer
     */
    unsigned int size(void) const {
        if(!full) {
            if(head >= tail)
                return head - tail;
            else
                return buffer_size+head-tail;
        }

        else
            return buffer_size;
    }

    /*!
     * The number of elements the Circular Buffer can holds
     * @return Returns the number of elements the Circular Buffer can holds
     */
    unsigned int capacity(void) const {
        return buffer_size;
    }

    /*!
     * Inserts an element in the circular buffer
     * @param  element  Element to be inserted
     */
    void push(const BufferType& element) {
        if(full())
            buffer[head]->~BufferType();
        buffer[head] = element;
        advance();
    }

    /*!
     * Removes the oldest element from the circular buffer
     * @return  The removed element from the buffer
     */
    BufferType& pop(void) {
        if(empty())
            return BufferType();

        auto val = buffer[tail];
        retreat();
        return val;
    }

    /*!
     * Pushs an element to the buffer
     * @param element Element to be inserted
     */
    void operator+=(const BufferType& element) {
        push(element);
    }

    BufferType& operator[](unsigned int index) {
        return buffer[(head+index)%buffer_size];
    }


    template <typename T>
    class _iterator {
        friend class CircularBuffer;
    public:
        _iterator (CircularBuffer& cb, unsigned int head, unsigned int size) : _cb(cb), _head(head), _size(size) {}

        T& operator* (void) { return _cb.buffer[_head]; }
        T operator& (void) { return &_cb.buffer[_head]; }
        _iterator& operator++ (void) { _head = (_head+1)%_cb.buffer_size; _size++; return *this; }
        _iterator& operator-- (void) { _head = (_head-1)%_cb.buffer_size; _size--; return *this; }
        bool operator!= (const _iterator& it) { return _size != it._size; }
        bool operator== (const _iterator& it) { return _size == it._size; }

    private:
        CircularBuffer& _cb;
        unsigned int _head;
        unsigned int _size;
    };

    typedef _iterator<BufferType> iterator;
    _iterator<BufferType> begin() { return _iterator<BufferType>(*this, tail, 0); }
    _iterator<BufferType> end() { return _iterator<BufferType>(*this, head, size()); }
};

class Metrics {
    typedef enum {
        TYPE_INT = 0,
        TYPE_FLOAT,
        TYPE_BYTE_ARRAY,
        TYPE_DATE_TIME
    } metrics_type;

    typedef struct metric_buffer {
        metrics_type type;
        BaseBuffer* buffer;
        metric_buffer(metrics_type t, BaseBuffer* buf = nullptr) : type(t), buffer(buf) {};
    } metric_buffer_t;

    template <typename Type> struct buffer_element {
        Type value;
        int64_t tv_sec;
        int64_t tv_usec;
        int64_t seq;

        buffer_element(Type val, int64_t sec, int64_t usec, uint64_t seq) : value(val), tv_sec(sec), tv_usec(usec), seq(seq) { };
    };

    typedef struct buffer_element_int : public buffer_element<int64_t> {
        buffer_element_int(int64_t val, int64_t sec, int64_t usec, uint64_t seq) : buffer_element(val, sec, usec, seq) {};
    } buffer_element_int;

    typedef struct buffer_element_float : public buffer_element<float> {
        buffer_element_float(float val, int64_t sec, int64_t usec, uint64_t seq) : buffer_element(val, sec, usec, seq) {};
    } buffer_element_float;

    typedef struct buffer_element_byte_array : public buffer_element<int8_t*> {
        int32_t size;
        buffer_element_byte_array(int8_t* val, int64_t sec, int64_t usec, uint64_t seq, int32_t size) : buffer_element(val, sec, usec, seq), size(size) {};
        ~buffer_element_byte_array(void) {
            delete value;
        }
    } buffer_element_byte_array;

    typedef struct buffer_element_date_time : public buffer_element<struct timespec> {
        buffer_element_date_time(struct timespec& val, int64_t sec, int64_t usec, uint64_t seq) : buffer_element(val, sec, usec, seq) {
            std::memcpy(&this->value, &val, sizeof(struct timespec));
        };
    } buffer_element_date_time;

    std::map<std::string, metric_buffer_t*> metric_buffers;
    unsigned int default_size;
    uint64_t sequence_no = 0;

    void _set_metric(const char* metric_id, void* data, metrics_type type = TYPE_INT, int size = 0) {
        auto map_it = metric_buffers.find(metric_id);

        if(map_it == metric_buffers.end())
            throw std::runtime_error("Requested metric does not exists.");

        auto buf = *(map_it->second);
        if(buf.type != type)
            throw std::runtime_error("Requested metric is inserted with different types.");

        struct timespec start;
        clock_gettime(CLOCK_REALTIME, &start);
        return;
        switch (type) {
            case TYPE_INT:
            {
                auto element = new buffer_element_int(*((int64_t*)(data)), (int64_t) start.tv_sec, (int64_t) start.tv_nsec, sequence_no);
                auto buffer_i = reinterpret_cast<CircularBuffer<buffer_element_int*>*>(buf.buffer);
                buffer_i->push(element);
                break;
            }

            case TYPE_FLOAT:
            {
                auto element = new buffer_element_float(*((float*)(data)), (int64_t) start.tv_sec, (int64_t) start.tv_nsec, sequence_no);
                auto buffer_f = reinterpret_cast<CircularBuffer<buffer_element_float*>*>(buf.buffer);
                buffer_f->push(element);
                break;
            }

            case TYPE_BYTE_ARRAY:
            {
                auto element = new buffer_element_byte_array((int8_t*) data, (int64_t) start.tv_sec, (int64_t) start.tv_nsec, sequence_no, size);
                auto buffer_b = reinterpret_cast<CircularBuffer<buffer_element_byte_array*>*>(buf.buffer);
                buffer_b->push(element);
                break;
            }

            case TYPE_DATE_TIME:
            {
                struct timespec* t = reinterpret_cast<struct timespec*>(data);
                auto element = new buffer_element_date_time(*t, (int64_t) start.tv_sec, (int64_t) start.tv_nsec, sequence_no);
                auto buffer_dt = reinterpret_cast<CircularBuffer<buffer_element_date_time*>*>(buf.buffer);
                buffer_dt->push(element);
                break;
            }

            default:
                throw std::runtime_error("Invalid type requested.");
                break;
        }

        sequence_no++;
    }

public:
    /*!
     * Create the metrics
     * @param N Size of the Circular Buffers of each metric
     */
    Metrics(unsigned int N=1) : default_size(N) {
        metric_buffers["current_time"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["start_time"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["tasks_received"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["tasks_executed"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["results_sent"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["total_stalling_time"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["total_executing_time"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["init_time"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["tasks_generated"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["run_iterations"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["tasks_sent"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["tasks_replicated"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["tasks_commited"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["results_received"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));
        metric_buffers["results_discarded"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)));

        metric_buffers["test_i"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_INT, new CircularBuffer<buffer_element_int*>(N)) );
        metric_buffers["test_f"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_FLOAT, new CircularBuffer<buffer_element_float*>(N)) );
        metric_buffers["test_dt"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_DATE_TIME, new CircularBuffer<buffer_element_date_time*>(N)) );
        metric_buffers["test_ba"] = std::unique_ptr<metric_buffer_t>(new metric_buffer_t(TYPE_BYTE_ARRAY, new CircularBuffer<buffer_element_byte_array*>(N)) );
    } ;

    ~Metrics(void) {
        clear();
        metric_buffers.clear();
    }

    std::vector<std::string>* get_metrics_list (void) {
        std::vector<std::string>* metrics_list = new std::vector<std::string>();

        for (auto const& key : metric_buffers)
            metrics_list->push_back(key.first);

        return metrics_list;
    }

    unsigned int get_metric_size(const char* metric_id) const {
        return default_size;
    }


    void set_metric_int(const char* metric_id, int64_t& data) {
        _set_metric(metric_id, reinterpret_cast<void*>(&data), TYPE_INT);
    }

    void set_metric_float(const char* metric_id, float& data) {
        _set_metric(metric_id, reinterpret_cast<void*>(&data), TYPE_FLOAT);
    }

    void set_metric_datetime(const char* metric_id, struct timespec& data) {
        _set_metric(metric_id, reinterpret_cast<void*>(&data), TYPE_DATE_TIME);
    }

    void set_metric_byte_array(const char* metric_id, int8_t* data, int size) {
        _set_metric(metric_id, reinterpret_cast<void*>(data), TYPE_BYTE_ARRAY, size);
    }

    void serializate(spits::ostream& stream) {
        std::vector<std::string> metrics_list;

        for (auto const& key : metric_buffers)
            metrics_list.push_back(key.first);

        serializate(stream, metrics_list);
    }

    void serializate(spits::ostream& stream, std::vector<std::string>& metrics_list) {
        for (auto s : metrics_list) {
            int k = 0;
            auto map_it = metric_buffers.find(s);

            if(map_it == metric_buffers.end())
                throw std::runtime_error("Metric not recognized to Serialize");

            auto buf = *(map_it->second);

            if(buf.type == TYPE_INT) {
                auto buffer_i = reinterpret_cast<CircularBuffer<buffer_element_int*>*>(buf.buffer);
                stream << (uint64_t) buffer_i->size();

                for(auto element = buffer_i->begin(); element != buffer_i->end(); ++element) {
                    stream << (int64_t) (*element)->value;
                    k++;
                    std::cout << "\t[" << k << "] Serialized element INT> " << (int64_t) (*element)->value << std::endl;
                }
            }

            else if(buf.type == TYPE_FLOAT) {
                auto buffer_f = reinterpret_cast<CircularBuffer<buffer_element_float*>*>(buf.buffer);
                stream << (uint64_t) buffer_f->size();

                for(auto element = buffer_f->begin(); element != buffer_f->end(); ++element) {
                    stream << (float) (*element)->value;
                    k++;
                    std::cout << "\t[" << k << "] Serialized element FLOAT> " << (float) (*element)->value << std::endl;
                }
            }

            else if(buf.type == TYPE_BYTE_ARRAY) {
                auto buffer_b = reinterpret_cast<CircularBuffer<buffer_element_byte_array*>*>(buf.buffer);
                stream << (uint64_t) buffer_b->size();

                for(auto element = buffer_b->begin(); element != buffer_b->end(); ++element) {
                    std::string s(reinterpret_cast<const char*>((*element)->value), (size_t) (*element)->size );
                    stream << s;
                    k++;
                    std::cout << "\t[" << k << "] Serialized element BYTE_ARRAY> \'" << s << "\' (with size: " << (*element)->size << ")" << std::endl;
                }
            }

            else if(buf.type == TYPE_DATE_TIME) {
                auto buffer_dt = reinterpret_cast<CircularBuffer<buffer_element_date_time*>*>(buf.buffer);
                stream << (uint64_t) buffer_dt->size();

                for(auto element = buffer_dt->begin(); element != buffer_dt->end(); ++element) {
                    stream.write_data((const void*) &(*element)->value, sizeof(struct timespec));
                    k++;
                    std::cout << "\t[" << k << "] Serialized element DATE/TIME> " << (int64_t) (*element)->value.tv_sec << ":" << (int64_t) (*element)->value.tv_nsec << std::endl;
                }
            }

            else
                throw std::runtime_error("Not a valid type to serialize.");

            std::cout << "Serialized " << k << " elements for \'" << map_it->first  << "\' (actual stream position: " << stream.pos() << ")" << std::endl;
        }
        //    std::cout << "Got Metric: \'" << s << "\'" << std::endl;
    }

    void clear(void) {
        for (auto const& key : metric_buffers) {
            metric_buffer_t& b = *(key.second);

            if(b.type == TYPE_BYTE_ARRAY) {
                auto c = reinterpret_cast<CircularBuffer<buffer_element_byte_array*>*>(b.buffer);
                while(!c->is_empty()) {
                    auto k = c->pop();
                    delete k;
                }
                delete c;
            }
            else if(b.type == TYPE_DATE_TIME) {
                auto c = reinterpret_cast<CircularBuffer<buffer_element_date_time*>*>(b.buffer);
                delete c;
            }
            else if(b.type == TYPE_INT) {
                auto c = reinterpret_cast<CircularBuffer<buffer_element_int*>*>(b.buffer);
                delete c;
            }
            else if(b.type == TYPE_FLOAT) {
                auto c = reinterpret_cast<CircularBuffer<buffer_element_float*>*>(b.buffer);
                delete c;
            }
            //delete &(metric_buffers[key.first]);
        }
    }
};


#endif /* __UTILS_H__ */
