#ifndef METRICS_H_
#define METRICS_H_

#ifdef __cplusplus
extern "C" {
#endif


enum DataType {
  TYPE_INT = 0,
  TYPE_FLOAT = 1,
  TYPE_STRING = 2,
  TYPE_BYTES = 3
};


/* C Wrapper */
struct buffer_info {
  char* name;
  enum DataType type;
  unsigned int capacity;
};

struct buffer_infos {
  unsigned int quantity;
  struct buffer_info* infos;
};

struct metric_info {
  union {
    unsigned char* c_data;
    int i_data;
    float f_data;
  };

  unsigned int data_size;
  unsigned long seconds;
  unsigned long nano_seconds;
  unsigned int sequence_no;
};

struct buffer_metrics {
  char* name;
  unsigned int type;
  unsigned int num_metrics;
  struct metric_info* metrics;
};


void* spits_metrics_new(unsigned int buffer_size);

void spits_set_metric_int(void* metric_manager, const char* name, \
      int value);

void spits_set_metric_float(void* metric_manager, const char* name, \
      float value);

void spits_set_metric_string(void* metric_manager, const char* name, \
      char* value);

void spits_set_metric_bytes(void* metric_manager, const char* name, \
    unsigned char* value, unsigned int size);

unsigned int spits_get_num_buffers(void* metric_manager);

void spits_metrics_delete(void* metric_manager);

struct buffer_infos* spits_get_metrics_list(void* metric_manager);

void spits_free_metric_list(struct buffer_infos* buffers);

struct buffer_metrics* spits_get_metrics_history(void* metric_manager, \
    unsigned int n_metrics, const char** metrics_list, unsigned int* sizes);

void spits_free_metrics_history(struct buffer_metrics* metrics, \
    unsigned int n_metrics);

#ifdef __cplusplus
}
#endif
#endif /* METRICS_H_ */
