/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Edson Borin <edson@ic.unicamp.br>
 * Copyright (c) 2015 Caian Benedicto <caian@ggaunicamp.com>
 * Copyright (c) 2014 Ian Liu Rodrigues <ian.liu@ggaunicamp.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#ifndef __METRICS_H__
#define __METRICS_H__

#ifdef __cplusplus
extern "C" {
#endif

/* Metrics */

void* spits_metrics_new(int buffer_size);

void spits_set_metric_int(void* metrics, const char* metric_name, int ivalue);

void spits_set_metric_float(void* metrics, const char* metric_name, float fvalue) ;

void spits_set_metric_double(void* metrics, const char* metric_name, double dvalue);

void spits_set_metric_string(void* metrics, const char* metric_name, char* string);

void spits_set_metric_bytes(void* metrics, const char* metric_name, unsigned char* byte_array, int size);

int spits_get_num_buffers(void* metrics);

void* spits_get_metrics_list(void* metrics);

void* spits_get_metrics_last_values(void* metrics, const char** metrics_list);

void* spits_get_metrics_history(void* metrics, const char** metrics_list, int* sizes);

void spits_free_ptr(void* p);

void spits_metrics_reset(void* metrics);

void spits_metrics_delete(void* metrics);

void spits_metrics_debug_dump(void* metrics);


#ifdef __cplusplus
}
#endif

#endif /* __METRICS_H__ */
