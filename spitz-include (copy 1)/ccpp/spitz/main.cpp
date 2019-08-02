#include <iostream>
#include <random>
#include <cstdint>
#include <map>
#include <vector>
#include <memory>
#include <exception>
#include <cstring>
#include <ctime>
//#include "utils.hpp"
//#include "stream.hpp"

static __inline__ unsigned long long rdtsc(void)
{
  unsigned hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

void gen_random(char *s, const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

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

int main(int argc, char** argv) {
    if(argc != 2) {
        std::cerr << "Error: No params!" << std::endl;
        return -1;
    }

    //Metrics M(atoi(argv[1]));
    srand(rdtsc());

    /*for (int64_t i=1; i<=15; ++i) {
        int64_t k = i*100;
        M.set_metric_int("test_i", k);
    }

    for (int64_t i=1; i<=15; ++i) {
        float k = i*2.5;
        M.set_metric_float("test_f", k);
    }

    for (int64_t i=1; i<=15; ++i) {
        struct timespec tm;
        clock_gettime(CLOCK_REALTIME, &tm);
        tm.tv_sec += i*101;
        M.set_metric_datetime("test_dt", tm);
    }

    for (int64_t i=1; i<=15; ++i) {
        char* ch = new char[i*2+1];
        gen_random(ch, i*2-1);
        M.set_metric_byte_array("test_ba", (int8_t*) ch, i*2-1);
    }

    spitz::ostream stream;
    std::vector<std::string> keys({"test_i", "test_f", "test_dt", "test_ba"});
    M.serializate(stream, keys);

    spitz::istream destream(stream.data(), stream.pos());
    int size;

    size = destream.read_ulonglong();
    for (int i=0; i<size; ++i) {
        std::cout << "Deserialized INT> " << destream.read_longlong() << std::endl;
    }

    size = destream.read_ulonglong();
    for (int i=0; i<size; ++i) {
        std::cout << "Deserialized FLOAT> " << destream.read_float() << std::endl;
    }

    size = destream.read_ulonglong();
    for (int i=0; i<size; ++i) {
        struct timespec tm;
        destream.read_data((void*) &tm, sizeof(struct timespec));
        std::cout << "Deserialized DATE/TIME> " << tm.tv_sec << " : " << tm.tv_nsec << std::endl;
    }

    size = destream.read_ulonglong();
    for (int i=0; i<size; ++i) {
        std::cout << "Deserialized STRING> " << destream.read_string() << std::endl;
    }*/
    /*spitz::istream stream_i(stream.data(), stream.pos());
    while(stream_i.has_data()) {
        std::cout << stream_i.read_longlong() << std::endl;
    }*/

    return 0;
}
