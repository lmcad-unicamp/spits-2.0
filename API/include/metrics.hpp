#ifndef METRICS_HPP_
#define METRICS_HPP_

#include "metrics.h"

#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <tuple>
#include <deque>
#include <map>
#include <chrono>
#include <stdexcept>
#include <mutex>
#include <shared_mutex>
#include <cstddef>

#define DEFAULT_CAPACITY 10

//For high_resolution_clock
using namespace std::chrono;

struct MetricElementBase {
  unsigned long sequence;
  high_resolution_clock::time_point timestamp;
  MetricElementBase(unsigned long seq, high_resolution_clock::time_point tstamp) : \
    sequence(seq), timestamp(tstamp) {}
};

// Classes and structs
template <typename T>
struct MetricElement : public MetricElementBase {
  T value;

  MetricElement() {};
  MetricElement(const T& val, unsigned long sequence, \
      high_resolution_clock::time_point tstamp) : \
      MetricElementBase(sequence, tstamp), value(val) {}

  MetricElement(const struct MetricElement& other) : \
      MetricElementBase(other.sequence, other.timestamp), value(other.value) {}

  MetricElement& operator=(const struct MetricElement& other) {
    if (this != &other) {
      value = other.value;
      sequence = other.sequence;
      timestamp = other.timestamp;
    }
    return *this;
  }
};

// Simple ring buffer
template <typename T>
class Ring : public std::deque<T> {
  using container_type = std::deque<T>;
public:
    Ring(unsigned int capacity = 1) : _capacity(capacity) {}
    void push_back(const T& value) {
        if (this->size() == this->_capacity) {
          this->pop_front();
        }
        container_type::push_back(value);
    }

    ~Ring(void){
      this->clear();
    }

private:
  unsigned int _capacity;
};

// Base class to allow polymorphism
class MetricBufferBase {
public:
  MetricBufferBase(enum DataType dtype, unsigned int capacity, unsigned int seq) : \
   _dtype(dtype), _capacity(capacity), _sequence(seq) {}

  virtual ~MetricBufferBase(void) {};

  unsigned int get_capacity(void) const {
    return _capacity;
  }

  unsigned int get_sequence(void) const {
    return _sequence;
  }

  enum DataType get_dtype(void) const {
    return _dtype;
  }

  virtual unsigned int size(void) const = 0;

protected:
  enum DataType _dtype;
  unsigned int _capacity;
  unsigned int _sequence;
};

// Buffer of metric elements
template <typename T, enum DataType U>
class MetricBuffer : public MetricBufferBase {
public:
  using element_type = MetricElement<T>;

  MetricBuffer(unsigned int capacity=1, unsigned int seq = 0) : \
    MetricBufferBase(U, capacity, seq), _buffer(capacity) {
      if (capacity == 0) {
        throw std::length_error("Buffer was initialized with 0 capacity");
      }
    }

  void add(const T& value) {
    high_resolution_clock::time_point time_now = high_resolution_clock::now();
    std::unique_lock lock(_mutex);
    _buffer.push_back(element_type(value, _sequence++, time_now));
  }

  std::vector<element_type> retrieve_multiple(const unsigned int n_elements) {
    std::vector<element_type> elements;
    if (n_elements > size()) {
      throw std::out_of_range("Requested element range is higher than queue size");
    }

    std::shared_lock lock(_mutex);
    for (unsigned int i = 0; i<n_elements; ++i) {
      elements.push_back(_buffer[i]);
    }

    return elements;
  }

  unsigned int size(void) const {
    std::shared_lock lock(_mutex);
    return _buffer.size();
  }

  void reset(void) {
    std::unique_lock lock(_mutex);
    _buffer.clear();
  }

  ~MetricBuffer(void) {
    reset();
  }

private:
  Ring<element_type> _buffer;
  mutable std::shared_mutex _mutex;
};


// Default buffer types: int, float, string and bytes
typedef MetricBuffer<int, TYPE_INT> int_buffer;
typedef MetricBuffer<float, TYPE_FLOAT> float_buffer;
typedef MetricBuffer<std::string, TYPE_STRING> string_buffer;
typedef MetricBuffer<std::vector<std::byte>, TYPE_BYTES> byte_buffer;

// Manage multiple metric Buffers
class MetricManager {
public:
  MetricManager(unsigned int capacity = DEFAULT_CAPACITY) : \
    _default_capacity(capacity) {}

  ~MetricManager(void) {
    reset();
  }

  unsigned int get_default_capacity(void) const {
    return _default_capacity;
  }

  void set_default_capacity(const unsigned int capacity) {
    _default_capacity = capacity;
  }

  unsigned int get_num_buffers(void) const {
    return _metric_map.size();
  }

  unsigned int get_buffer_capacity(const std::string name) {
    return _metric_map.at(name)->get_capacity();
  }

  enum DataType get_buffer_dtype(const std::string name) {
    return _metric_map.at(name)->get_dtype();
  }

  std::vector<std::string> get_buffer_names(void) {
    std::vector<std::string> names;
    for (auto const& it : _metric_map) { names.push_back(it.first); }
    return names;
  }

  unsigned int get_buffer_size(const std::string name) {
    MetricBufferBase* buffer = _metric_map.at(name);
    return buffer->size();
  }

  void add_metric(const std::string name, const int value) {
    _add_metric<int_buffer, int, TYPE_INT>(name, value);
  }

  void add_metric(const std::string name, const float value) {
    _add_metric<float_buffer, float, TYPE_FLOAT>(name, value);
  }

  void add_metric(const std::string name, const std::string& value) {
    _add_metric<string_buffer, std::string, TYPE_STRING>(name, value);
  }

  void add_metric(const std::string name, const std::vector<std::byte>& value) {
    _add_metric<byte_buffer, std::vector<std::byte>, TYPE_BYTES>(name, value);
  }

  #define GET_ELEMENT(BUFFER_TYPE,TYPE,NAME) \
    std::vector<MetricElement<TYPE>> get_element_##NAME (const std::string name, \
        const unsigned int n_elements = 1)  { \
      return get_elements<BUFFER_TYPE,MetricElement<TYPE>>(name, n_elements); \
  }
  GET_ELEMENT(int_buffer,int,int);
  GET_ELEMENT(float_buffer,float,float);
  GET_ELEMENT(string_buffer,std::string,string);
  GET_ELEMENT(byte_buffer,std::vector<std::byte>,bytes);

private:
  unsigned int _default_capacity;
  std::map<std::string, MetricBufferBase*> _metric_map;

  // Add metric template
  template<typename BufferType, typename MetricElementType, enum DataType U>
  void _add_metric(const std::string name, const MetricElementType& value) {
    if (name.empty()) { throw std::invalid_argument("Invalid buffer name"); }
    auto it = _metric_map.find(name);
    BufferType* buffer;
    if (it == _metric_map.end()) {
      buffer = new BufferType(_default_capacity);
      _metric_map[name] = buffer;
    }
    else {
      buffer = (BufferType*) it->second;
      if(buffer->get_dtype() != U) {
        throw std::invalid_argument("Invalid type for metric buffer");
      }
    }
    buffer->add(value);
  }

  template<typename BufferType, typename MetricElementType>
  std::vector<MetricElementType> get_elements(const std::string name, \
      unsigned int n_elements) {
    BufferType* buffer = (BufferType*) _metric_map.at(name);
    return buffer->retrieve_multiple(n_elements);
  }

  void reset(void) {
    for (auto it = _metric_map.begin(); it != _metric_map.end(); it++) {
      delete it->second;
    }
    _metric_map.clear();
  }
};

namespace spits {
  typedef MetricManager metrics;
}


#ifdef SPITS_ENTRY_POINT
// Extern C
extern "C" void* spits_metrics_new(unsigned int buffer_size) {
  return static_cast<void*>(new MetricManager(buffer_size));
}

extern "C" void spits_set_metric_int(void* metric_manager, const char* name, \
      int value) {
  MetricManager* m = static_cast<MetricManager*>(metric_manager);
  m->add_metric(name, value);
}

extern "C" void spits_set_metric_float(void* metric_manager, const char* name, \
      float value) {
  MetricManager* m = static_cast<MetricManager*>(metric_manager);
  m->add_metric(name, value);
}

extern "C" void spits_set_metric_string(void* metric_manager, const char* name, \
      char* value) {
  MetricManager* m = static_cast<MetricManager*>(metric_manager);
  m->add_metric(name, std::string(value));
}

extern "C" void spits_set_metric_bytes(void* metric_manager, const char* name, \
    unsigned char* value, unsigned int size) {
  MetricManager* m = static_cast<MetricManager*>(metric_manager);
  //std::vector<std::byte> b_vector(value, value+size);
  //m->add_metric(name, b_vector);
}

extern "C" unsigned int spits_get_num_buffers(void* metric_manager) {
  MetricManager* m = static_cast<MetricManager*>(metric_manager);
  return m->get_num_buffers();
}

extern "C" void spits_metrics_delete(void* metric_manager) {
  MetricManager* m = static_cast<MetricManager*>(metric_manager);
  if (m) delete(m);
}

extern "C" struct buffer_infos* spits_get_metrics_list(void* metric_manager) {
  MetricManager* m = static_cast<MetricManager*>(metric_manager);
  std::vector<std::string> names = m->get_buffer_names();
  struct buffer_info* b_infos = new buffer_info[names.size()];
  for(unsigned int i=0; i<names.size(); ++i) {
    int s = names[i].length();
    b_infos[i].name = new char[s+1];
    strcpy(b_infos[i].name, names[i].c_str());
    b_infos[i].type = m->get_buffer_dtype(names[i]);
    b_infos[i].capacity = m->get_buffer_capacity(names[i]);
  }
  struct buffer_infos* infos = new buffer_infos;
  infos->quantity = names.size();
  infos->infos = b_infos;
  return infos;
}

extern "C" void spits_free_metric_list(struct buffer_infos* buffers) {
  for (unsigned int i = 0; i<buffers->quantity; ++i) {
    delete[] buffers->infos[i].name;
  }
  delete[] buffers->infos;
  delete buffers;
}

extern "C" struct buffer_metrics* spits_get_metrics_history(void* metric_manager, \
    unsigned int n_metrics, const char** metrics_list, unsigned int* sizes) {
  MetricManager* m = static_cast<MetricManager*>(metric_manager);
  struct buffer_metrics* buffers = new buffer_metrics[n_metrics];
  for(unsigned int i = 0; i<n_metrics; ++i) {
    unsigned int buffer_sz = m->get_buffer_size(metrics_list[i]);
    unsigned int s = strlen(metrics_list[i]);
    buffers[i].name = new char[s+1];
    strcpy(buffers[i].name, metrics_list[i]);
    int dtype = m->get_buffer_dtype(metrics_list[i]);
    buffers[i].type = dtype;
    unsigned n_elements_to_pick = buffer_sz <= sizes[i]? buffer_sz : sizes[i];
    if (dtype == TYPE_INT) {
        auto values = m->get_element_int(metrics_list[i], n_elements_to_pick);
        buffers[i].num_metrics = values.size();
        struct metric_info* metrics = new metric_info[values.size()];
        for(unsigned int j = 0; j<values.size(); ++j) {
          metrics[j].i_data = values[j].value;
          metrics[j].data_size = sizeof(int);
          metrics[j].sequence_no = values[j].sequence;
          auto secs = time_point_cast<std::chrono::seconds>(values[j].timestamp);
          auto ns = time_point_cast<std::chrono::nanoseconds>(values[j].timestamp) - time_point_cast<std::chrono::nanoseconds>(secs);
          metrics[j].seconds = secs.time_since_epoch().count();
          metrics[j].nano_seconds = ns.count();
        }
        buffers[i].metrics = metrics;
    }
    else if (dtype == TYPE_FLOAT) {
        auto values = m->get_element_float(metrics_list[i], n_elements_to_pick);
        buffers[i].num_metrics = values.size();
        struct metric_info* metrics = new metric_info[values.size()];
        for(unsigned int j = 0; j<values.size(); ++j) {
          metrics[j].f_data = values[j].value;
          metrics[j].data_size = sizeof(float);
          metrics[j].sequence_no = values[j].sequence;
          auto secs = time_point_cast<std::chrono::seconds>(values[j].timestamp);
          auto ns = time_point_cast<std::chrono::nanoseconds>(values[j].timestamp) - time_point_cast<std::chrono::nanoseconds>(secs);
          metrics[j].seconds = secs.time_since_epoch().count();
          metrics[j].nano_seconds = ns.count();
        }
        buffers[i].metrics = metrics;
    }
    else if (dtype == TYPE_STRING) {
        auto values = m->get_element_string(metrics_list[i], n_elements_to_pick);
        buffers[i].num_metrics = values.size();
        struct metric_info* metrics = new metric_info[values.size()];
        for(unsigned int j = 0; j<values.size(); ++j) {
          metrics[j].data_size = values[j].value.length()+1;
          metrics[j].c_data = new unsigned char[metrics[j].data_size];
          strcpy((char*)metrics[j].c_data, values[j].value.c_str());
          metrics[j].sequence_no = values[j].sequence;
          auto secs = time_point_cast<std::chrono::seconds>(values[j].timestamp);
          auto ns = time_point_cast<std::chrono::nanoseconds>(values[j].timestamp) - time_point_cast<std::chrono::nanoseconds>(secs);
          metrics[j].seconds = secs.time_since_epoch().count();
          metrics[j].nano_seconds = ns.count();
        }
        buffers[i].metrics = metrics;
    }
    else if (dtype == TYPE_BYTES) {
      auto values = m->get_element_bytes(metrics_list[i], n_elements_to_pick);
      buffers[i].num_metrics = values.size();
      struct metric_info* metrics = new metric_info[values.size()];
      for(unsigned int j = 0; j<values.size(); ++j) {
        metrics[j].data_size = values[j].value.size();
        metrics[j].c_data = new unsigned char[metrics[j].data_size];
        memcpy(metrics[j].c_data, values[j].value.data(), metrics[j].data_size);
        metrics[j].sequence_no = values[j].sequence;
        auto secs = time_point_cast<std::chrono::seconds>(values[j].timestamp);
        auto ns = time_point_cast<std::chrono::nanoseconds>(values[j].timestamp) - time_point_cast<std::chrono::nanoseconds>(secs);
        metrics[j].seconds = secs.time_since_epoch().count();
        metrics[j].nano_seconds = ns.count();
      }
      buffers[i].metrics = metrics;
    }
    else {
      buffers[i].metrics = NULL;
    }
  }
  return buffers;
}

extern "C" void spits_free_metrics_history(struct buffer_metrics* metrics, \
    unsigned int n_metrics) {
  for(unsigned int i = 0; i<n_metrics; ++i) {
    if (metrics[i].type == TYPE_STRING || metrics[i].type == TYPE_BYTES){
      for (unsigned int j=0; j<metrics[i].num_metrics; ++j) {
        delete[] metrics[i].metrics[j].c_data;
      }
    }
    delete[] metrics[i].name;
    delete[] metrics[i].metrics;
  }
  delete[] metrics;
}

#endif /* SPITS_ENTRY_POINT */
#endif /* METRICS_HPP_ */
