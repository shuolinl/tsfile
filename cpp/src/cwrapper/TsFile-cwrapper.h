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

#ifndef CWRAPPER_TSFILE_CWRAPPER_H
#define CWRAPPER_TSFILE_CWRAPPER_H

#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#ifdef _WIN32
#include <sys/stat.h>
#endif

typedef long long SchemaInfo;
typedef long long timestamp;
typedef void* CTsFileReader;
typedef void* CTsFileWriter;
typedef void* TsFileRowData;
typedef int ErrorCode;
typedef void* TimeFilterExpression;

// DATA TYPE
#define TS_TYPE_INT32 1 << 8
#define TS_TYPE_BOOLEAN 1 << 9
#define TS_TYPE_FLOAT 1 << 10
#define TS_TYPE_DOUBLE 1 << 11
#define TS_TYPE_INT64 1 << 12
#define TS_TYPE_TEXT 1 << 13

// ENCODING TYPE
#define TS_ENCODING_PLAIN 1 << 16
#define TS_ENCODING_TS_DIFF 1 << 17
#define TS_ENCODING_DICTIONARY 1 << 18
#define TS_ENCODING_RLE 1 << 19
#define TS_ENCODING_BITMAP 1 << 20
#define TS_ENCODING_GORILLA_V1 1 << 21
#define TS_ENCODING_REGULAR 1 << 22
#define TS_ENCODING_GORILLA 1 << 23
#define TS_ENCODING_ZIGZAG 1 << 24
#define TS_ENCODING_FREQ 1 << 25

// COMPRESS TYPE
#define TS_COMPRESS_UNCOMPRESS 1LL << 32
#define TS_COMPRESS_SNAPPY 1LL << 33
#define TS_COMPRESS_GZIP 1LL << 34
#define TS_COMPRESS_LZO 1LL << 35
#define TS_COMPRESS_SDT 1LL << 36
#define TS_COMPRESS_PAA 1LL << 37
#define TS_COMPRESS_PLA 1LL << 38
#define TS_COMPRESS_LZ4 1LL << 39

#define MAX_COLUMN_FILTER_NUM 10

typedef struct column_schema {
    char* name;
    SchemaInfo column_def;
} ColumnSchema;

typedef struct table_shcema {
    char* table_name;
    ColumnSchema** column_schema;
    int column_num;
} TableSchema;

typedef enum operator_type {
    LT,
    LE,
    EQ,
    GT,
    GE,
    NOTEQ,
} OperatorType;

typedef enum expression_type {
    OR,
    AND,
    GLOBALTIME,
} ExpressionType;

typedef struct constant {
    int64_t value_condition;
    int type;
} Constant;

typedef struct expression {
    const char* column_name;
    Constant const_condition;
    ExpressionType expression_type;
    OperatorType operatype;
    struct expression* children[MAX_COLUMN_FILTER_NUM];
    int children_length;
} Expression;

/* A tablet data of one device, the tablet contains multiple measurements of
 * this device that share the same timestamp array.
 * Use create_tablet to create a tablet.
 * Use add_column_to_tablet to add a column to the tablet.
 * Use destory_tablet to free the tablet.
 */
typedef struct tablet {
    char* table_name;              // device path.
    ColumnSchema** column_schema;  // measurement schema.
    int column_num;                // number of columns.
    timestamp* times;              // timestamp array.
    bool** bitmap;                 // bitmap array.
    void** value;                  // value array.
    int cur_num;                   // current number of rows.
    int max_capacity;              // max capacity of the tablet.
} Tablet;

typedef struct tsfile_conf {
    int mem_threshold_kb;
} TsFileConf;

typedef Tablet DataResult;

typedef void* QueryDataRetINTERNAL;
typedef struct query_data_ret {
    char** column_names;
    int column_num;
    QueryDataRetINTERNAL data;
}* QueryDataRet;

#ifdef __cplusplus
extern "C" {
#endif

/* ---------- Tablet ---------- */

// create a tablet with the given table name and max capacity.
Tablet* create_tablet(const char* table_name, int max_capacity);

// add a column to the tablet and malloc memory for the column.
Tablet* add_column_to_tablet(Tablet* tablet, char* column_name,
                             SchemaInfo column_def);

// insert a data into the tablet with the given line id and column name.
ErrorCode add_data_to_tablet_i64(Tablet* tablet, int line_id,
                                 const char* column_name, int64_t value);
ErrorCode add_data_to_tablet_i32(Tablet* tablet, int line_id,
                                 const char* column_name, int32_t value);
ErrorCode add_data_to_tablet_bool(Tablet* tablet, int line_id,
                                  const char* column_name, bool value);
ErrorCode add_data_to_tablet_float(Tablet* tablet, int line_id,
                                   const char* column_name, float value);
ErrorCode add_data_to_tablet_double(Tablet* tablet, int line_id,
                                    const char* column_name, double value);
ErrorCode add_data_to_tablet_text(Tablet* tablet, int line_id,
                                  const char* column_name, const char* value);

// free the tablet and all the memory it used.
ErrorCode destory_tablet(Tablet* tablet);

/* ---------- Row ---------- */

// create a TsFileRowData with the given table name, timestamp and column
// length. TsFileRowData cantains the data of one device with the same
// timestamp.
TsFileRowData create_tsfile_row(const char* tablename, int64_t timestamp,
                                int column_length);

ErrorCode insert_data_into_tsfile_row_int32(TsFileRowData data, char* columname,
                                            int32_t value);
ErrorCode insert_data_into_tsfile_row_boolean(TsFileRowData data,
                                              char* columname, bool value);
ErrorCode insert_data_into_tsfile_row_int64(TsFileRowData data, char* columname,
                                            int64_t value);
ErrorCode insert_data_into_tsfile_row_float(TsFileRowData data, char* columname,
                                            float value);
ErrorCode insert_data_into_tsfile_row_double(TsFileRowData data,
                                             char* columname, double value);

/* ---------- TsFile ---------- */

/* -- reader --*/

// Open a TsFile reader with the given file path. If it fails, return NULL and
// set err_code.
CTsFileReader ts_reader_open(const char* pathname, ErrorCode* err_code);

// Close the TsFile reader. If it fails, return an error code.
ErrorCode ts_reader_close(CTsFileReader reader);

// If you need to construct a more complex AND expression that does not only
// filter a single sequence of time, you can use the following method to
// construct a complex expression.

TimeFilterExpression* create_andquery_timefilter();

TimeFilterExpression* create_time_filter(const char* table_name,
                                         const char* column_name,
                                         OperatorType oper, int64_t timestamp);

TimeFilterExpression* add_time_filter_to_and_query(
    TimeFilterExpression* exp_and, TimeFilterExpression* exp);

void destory_time_filter_query(TimeFilterExpression* expression);

// Query the data in the TsFile with expression.Return an iterator.
QueryDataRet ts_reader_query(CTsFileReader reader, const char* table_name,
                             const char** columns, int colum_num,
                             TimeFilterExpression* expression);

// Query the data of the measurements in the TsFile with the given time
// range.Return an iterator.
QueryDataRet ts_reader_begin_end(CTsFileReader reader, const char* table_name,
                                 char** columns, int colum_num, timestamp begin,
                                 timestamp end);

// Query all the data of the measurements in the TsFile. Return an iterator.
QueryDataRet ts_reader_read(CTsFileReader reader, const char* table_name,
                            char** columns, int colum_num);

// Get the data from the iterator. If the data is NULL, return NULL.
// Dataresult is tablet.
DataResult* ts_next(QueryDataRet data, int expect_line_count);

// print the data in dataresult or tablet.
void print_data_result(DataResult* result);

// Free the data iterator.
ErrorCode destory_query_dataret(QueryDataRet query_data_set);

/* -- writer --*/

// Open a TsFile writer with the given file path. If it fails, return NULL and
// set errcode.
CTsFileWriter ts_writer_open(const char* pathname, ErrorCode* err_code);

// Open a TsFile writer with the given file path and flag mode.
// O_CREAT | O_RDWR will add by default.
// If it fails, return NULL and set errcode.
CTsFileWriter ts_writer_open_flag(const char* pathname, mode_t flag,
                                  ErrorCode* err_code);

// Not support yet.
CTsFileWriter ts_writer_open_conf(const char* pathname, mode_t flag,
                                  ErrorCode* err_code, TsFileConf* conf);

// create timeseries.
ErrorCode tsfile_register_table_column(CTsFileWriter writer,
                                       const char* table_name,
                                       ColumnSchema* schema);

// create device with multiple timeseries.
ErrorCode tsfile_register_table(CTsFileWriter writer,
                                TableSchema* table_shcema);

ErrorCode tsfile_write_row_data(CTsFileWriter writer, TsFileRowData data);

ErrorCode ts_writer_close(CTsFileWriter writer);

ErrorCode destory_tsfile_row(TsFileRowData data);

ErrorCode tsfile_flush_data(CTsFileWriter writer);

Expression create_column_filter_I32(const char* column_name, OperatorType oper,
                                    int32_t int32_value);
Expression create_column_filter_I64(const char* column_name, OperatorType oper,
                                    int64_t int64_value);
Expression create_column_filter_bval(const char* column_name, OperatorType oper,
                                     bool bool_value);
Expression create_column_filter_fval(const char* column_name, OperatorType oper,
                                     float float_value);
Expression create_column_filter_dval(const char* column_name, OperatorType oper,
                                     double double_value);
Expression create_column_filter_cval(const char* column_name, OperatorType oper,
                                     const char* char_value);
#ifdef __cplusplus
}
#endif
#endif  // CWRAPPER_TSFILE_CWRAPPER_H
