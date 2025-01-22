/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <iostream>

typedef enum {
    TS_DATATYPE_BOOLEAN = 0,
    TS_DATATYPE_INT32 = 1,
    TS_DATATYPE_INT64 = 2,
    TS_DATATYPE_FLOAT = 3,
    TS_DATATYPE_DOUBLE = 4,
    TS_DATATYPE_TEXT = 5,
    TS_DATATYPE_VECTOR = 6,
    TS_DATATYPE_NULL_TYPE = 254,
    TS_DATATYPE_INVALID = 255
} TSDataType;

typedef enum {
    TS_ENCODING_PLAIN = 0,
    TS_ENCODING_DICTIONARY = 1,
    TS_ENCODING_RLE = 2,
    TS_ENCODING_DIFF = 3,
    TS_ENCODING_TS_2DIFF = 4,
    TS_ENCODING_BITMAP = 5,
    TS_ENCODING_GORILLA_V1 = 6,
    TS_ENCODING_REGULAR = 7,
    TS_ENCODING_GORILLA = 8,
    TS_ENCODING_ZIGZAG = 9,
    TS_ENCODING_FREQ = 10,
    TS_ENCODING_INVALID = 255
} TSEncoding;

typedef enum {
    TS_COMPRESSION_UNCOMPRESSED = 0,
    TS_COMPRESSION_SNAPPY = 1,
    TS_COMPRESSION_GZIP = 2,
    TS_COMPRESSION_LZO = 3,
    TS_COMPRESSION_SDT = 4,
    TS_COMPRESSION_PAA = 5,
    TS_COMPRESSION_PLA = 6,
    TS_COMPRESSION_LZ4 = 7,
    TS_COMPRESSION_INVALID = 255
} CompressionType;

typedef enum column_category { TAG, FIELD, ATTRIBUTE } ColumnCategory;

typedef struct column_schema {
    char* column_name;
    TSDataType data_type;
    ColumnCategory column_category;
} ColumnSchema;

typedef struct table_schema {
    char* table_name;
    ColumnSchema* column_schemas;
    int column_num;
} TableSchema;

typedef struct timeseries_schema {
    char* name;
    TSDataType data_type;
    TSEncoding encoding;
    CompressionType compression;
} TimeseriesSchema;

typedef struct device_schema {
    char* device_name;
    TimeseriesSchema** timeseries_schema;
    int timeseries_num;
} DeviceSchema;

typedef struct {
    char** column_names;
    TSDataType* data_types;
    uint32_t column_num;
} ResultSetMetaData;

typedef struct tsfile_conf {
    int mem_threshold_kb;
} TsFileConf;

typedef void* TsFileReader;
typedef void* TsFileWriter;

// just resue Tablet from c++
typedef void* Tablet;
typedef void* TsRecord;

typedef void* ResultSet;

typedef int32_t ERRNO;
typedef int64_t timestamp;

// Tablet API
Tablet tablet_new(const char* device_id, const char** column_name_list,
                  TSDataType* data_types, uint32_t column_num);

Tablet tablet_new(const char** column_name_list, TSDataType* data_types,
                  uint32_t column_num);

uint32_t tablet_get_cur_row_size(Tablet tablet);

void tablet_set_cur_row_size(Tablet tablet, uint32_t size);

ERRNO tablet_add_timestamp(Tablet tablet, uint32_t row_index,
                           timestamp timestamp);

#define tablet_add_value_by_name_(type)                                       \
    ERRNO tablet_add_value_by_name_##type(Tablet* tablet, uint32_t row_index, \
                                          char* column_name, type value);

tablet_add_value_by_name_(int32_t);
tablet_add_value_by_name_(int64_t);
tablet_add_value_by_name_(float);
tablet_add_value_by_name_(double);
tablet_add_value_by_name_(bool);

#define table_add_value_by_index_(type)                                        \
    ERRNO tablet_add_value_by_index_##type(Tablet* tablet, uint32_t row_index, \
                                           uint32_t column_index, type value);

table_add_value_by_index_(int32_t);
table_add_value_by_index_(int64_t);
table_add_value_by_index_(float);
table_add_value_by_index_(double);
table_add_value_by_index_(bool);

void* tablet_get_value(Tablet tablet, uint32_t row_index, uint32_t schema_index,
                       TSDataType& type);

// TsRecord API
TsRecord ts_record_new(const char* device_name, timestamp timestamp,
                       int timeseries_num);

#define insert_data_into_ts_record_by_name_(type)   \
    ERRNO insert_data_into_ts_record_by_name##type( \
        TsRecord data, char* measurement_name, type value);

insert_data_into_ts_record_by_name_(int32_t);
insert_data_into_ts_record_by_name_(int64_t);
insert_data_into_ts_record_by_name_(bool);
insert_data_into_ts_record_by_name_(float);
insert_data_into_ts_record_by_name_(double);

// TsFile reader and writer
TsFileReader tsfile_reader_new(const char* pathname, ERRNO* err_code);
TsFileWriter tsfile_writer_new(const char* pathname, ERRNO* err_code);
TsFileWriter tsfile_writer_open_conf(const char* pathname, mode_t flag,
                                     ERRNO* err_code, TsFileConf* conf);

ERRNO tsfile_writer_close(TsFileWriter writer);
ERRNO tsfile_reader_close(TsFileReader reader);

// register table or timeseries
void tsfile_writer_register_table(TsFileWriter writer, TableSchema* schema);
ERRNO tsfile_register_timeseries(TsFileWriter writer, const char* device_name,
                                 TimeseriesSchema* schema);
ERRNO tsfile_register_device(TsFileWriter writer, DeviceSchema* device_schema);

// write data
ERRNO tsfile_writer_write_tablet(TsFileWriter writer, Tablet tablet);
ERRNO tsfile_writer_write_ts_record(TsFileWriter writer, TsRecord record);

// flush data
ERRNO tsfile_writer_flush_data(TsFileWriter writer);
ERRNO tsfile_writer_close(TsFileWriter writer);

// query
ResultSet tsfile_reader_query(TsFileReader reader, char* table_name,
                              char** columns, uint32_t column_num,
                              timestamp start_time, timestamp end_time);
ResultSet tsfile_reader_query(TsFileReader reader, char** path_list,
                              uint32_t path_num, timestamp start_time,
                              timestamp end_time);
bool tsfile_result_set_has_next(ResultSet result_set);

#define tsfile_result_set_get_value_by_name_(type)                       \
    type tsfile_result_set_get_value_by_name##type(ResultSet result_set, \
                                                   char* column_name)
tsfile_result_set_get_value_by_name_(bool);
tsfile_result_set_get_value_by_name_(int32_t);
tsfile_result_set_get_value_by_name_(int64_t);
tsfile_result_set_get_value_by_name_(float);
tsfile_result_set_get_value_by_name_(double);

#define tsfile_result_set_get_value_by_index_(type)                        \
    type tsfile_result_set_get_value_by_index_##type(ResultSet result_set, \
                                                     uint32_t column_index);

tsfile_result_set_get_value_by_index_(int32_t);
tsfile_result_set_get_value_by_index_(int64_t);
tsfile_result_set_get_value_by_index_(float);
tsfile_result_set_get_value_by_index_(double);
tsfile_result_set_get_value_by_index_(bool);

#define tsfile_result_set_is_null_by_name_(type)                        \
    bool tsfile_result_set_is_null_by_name_##type(ResultSet result_set, \
                                                  char* column_name);

tsfile_result_set_is_null_by_name_(bool);
tsfile_result_set_is_null_by_name_(int32_t);
tsfile_result_set_is_null_by_name_(int64_t);
tsfile_result_set_is_null_by_name_(float);
tsfile_result_set_is_null_by_name_(double);

#define tsfile_result_set_is_null_by_index_(type)                        \
    bool tsfile_result_set_is_null_by_index_##type(ResultSet result_set, \
                                                   uint32_t column_index);

tsfile_result_set_is_null_by_index_(bool);
tsfile_result_set_is_null_by_index_(int32_t);
tsfile_result_set_is_null_by_index_(int64_t);
tsfile_result_set_is_null_by_index_(float);
tsfile_result_set_is_null_by_index_(double);

ResultSetMetaData tsfile_result_set_get_metadata(ResultSet result_set);

// Desc Table Schema

TableSchema tsfile_reader_get_table_schema(TsFileReader reader,
                                           const char* table_name);

// destroy pointer
ERRNO delete_tsfile_ts_record(TsRecord record);