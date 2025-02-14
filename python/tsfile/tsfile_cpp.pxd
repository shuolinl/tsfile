# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#cython: language_level=3
from libc.stdint cimport uint32_t, int32_t, int64_t

ctypedef int32_t ErrorCode

# import symbols from tsfile_cwrapper.h
cdef extern from "./tsfile_cwrapper.h":
    # common
    ctypedef int64_t timestamp

    # reader and writer etc
    ctypedef void* TsFileReader
    ctypedef void* TsFileWriter
    ctypedef void* Tablet
    ctypedef void* TsRecord
    ctypedef void* ResultSet

    # enum types
    ctypedef enum TSDataType:
        TS_DATATYPE_BOOLEAN = 0
        TS_DATATYPE_INT32 = 1
        TS_DATATYPE_INT64 = 2
        TS_DATATYPE_FLOAT = 3
        TS_DATATYPE_DOUBLE = 4
        TS_DATATYPE_TEXT = 5
        TS_DATATYPE_VECTOR = 6
        TS_DATATYPE_NULL_TYPE = 254
        TS_DATATYPE_INVALID = 255

    ctypedef enum TSEncoding:
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

    ctypedef enum CompressionType:
        TS_COMPRESSION_UNCOMPRESSED = 0,
        TS_COMPRESSION_SNAPPY = 1,
        TS_COMPRESSION_GZIP = 2,
        TS_COMPRESSION_LZO = 3,
        TS_COMPRESSION_SDT = 4,
        TS_COMPRESSION_PAA = 5,
        TS_COMPRESSION_PLA = 6,
        TS_COMPRESSION_LZ4 = 7,
        TS_COMPRESSION_INVALID = 255

    ctypedef enum ColumnCategory:
        TAG = 0,
        FIELD = 1

    # struct types
    ctypedef struct ColumnSchema:
        char* column_name
        TSDataType data_type
        ColumnCategory column_category

    ctypedef struct TableSchema:
        char* table_name
        ColumnSchema* column_schemas
        int column_num

    ctypedef struct TimeseriesSchema:
        char* timeseries_name
        TSDataType data_type
        TSEncoding encoding
        CompressionType compression

    ctypedef struct DeviceSchema:
        char* device_name
        TimeseriesSchema* timeseries_schema
        int timeseries_num

    ctypedef struct ResultSetMetaData:
        char** column_names
        TSDataType* data_types
        int column_num
        


    # Function Declarations

    # reader：new and close
    TsFileReader tsfile_reader_new(const char* pathname, ErrorCode* err_code);
    ErrorCode tsfile_reader_close(TsFileReader reader)

    # writer： new and close
    TsFileWriter tsfile_writer_new(const char * pathname, ErrorCode* err_code);
    ErrorCode tsfile_writer_close(TsFileWriter writer)

    # writer : register table, device and timeseries
    ErrorCode tsfile_writer_register_table(TsFileWriter writer, TableSchema* schema);
    ErrorCode tsfile_writer_register_timeseries(TsFileWriter writer, const char* device_name,
                                                TimeseriesSchema* schema);
    ErrorCode tsfile_writer_register_device(TsFileWriter writer, DeviceSchema* device_schema);

    # writer : write tablet data and flush
    ErrorCode tsfile_writer_write_tablet(TsFileWriter writer, Tablet tablet);
    ErrorCode tsfile_writer_write_ts_record(TsFileWriter writer, TsRecord record);

    ErrorCode tsfile_writer_flush_data(TsFileWriter writer);

    # tablet : new and add timestamp/value into tablet 
    Tablet tablet_new_with_device(const char* device_id, const char** column_name_list, TSDataType* data_types,
                                    int column_num, int max_rows);
    Tablet tablet_new(const char** column_names, TSDataType* data_types, int column_num);

    ErrorCode tablet_add_timestamp(Tablet tablet, uint32_t row_index, int64_t timestamp);
    ErrorCode tablet_add_value_by_index_int64_t(Tablet tablet, uint32_t row_index, uint32_t column_index, int64_t value);
    ErrorCode tablet_add_value_by_index_int32_t(Tablet tablet, uint32_t row_index, uint32_t column_index, int32_t value);
    ErrorCode tablet_add_value_by_index_double(Tablet tablet, uint32_t row_index, uint32_t column_index, double value);
    ErrorCode tablet_add_value_by_index_float(Tablet tablet, uint32_t row_index, uint32_t column_index, float value);
    ErrorCode tablet_add_value_by_index_bool(Tablet tablet, uint32_t row_index, uint32_t column_index, bint value);

    void free_tablet(Tablet* tablet);

    # row_record
    TsRecord ts_record_new(const char * device_id, int64_t timestamp, int timeseries_num);
    ErrorCode insert_data_into_ts_record_by_name_int32_t(TsRecord data, const char *measurement_name, int32_t value);
    ErrorCode insert_data_into_ts_record_by_name_int64_t(TsRecord data, const char *measurement_name, int64_t value);
    ErrorCode insert_data_into_ts_record_by_name_float(TsRecord data, const char *measurement_name, float value);
    ErrorCode insert_data_into_ts_record_by_name_double(TsRecord data, const char *measurement_name, double value);
    ErrorCode insert_data_into_ts_record_by_name_bool(TsRecord data, const char *measurement_name, bint value);

    void free_tsfile_ts_record(TsRecord* record);


    # resulSet : query data from tsfile reader
    ResultSet tsfile_reader_query_table(TsFileReader reader,
                                        const char* table_name,
                                        const char** columns, uint32_t column_num,
                                         int64_t start_time, int64_t end_time);
    ResultSet tsfile_reader_query_device(TsFileReader reader,
                                         const char * device_name,
                                         char** sensor_name, uint32_t sensor_num,
                                         timestamp start_time, timestamp end_time);

    # resultSet : get data from resultSet
    bint tsfile_result_set_has_next(ResultSet result_set);
    bint tsfile_result_set_is_null_by_index(ResultSet result_set, uint32_t column_index);
    bint tsfile_result_set_is_null_by_name(ResultSet result_set, const char* column_name);

    int32_t tsfile_result_set_get_value_by_index_int32_t(ResultSet result_set, uint32_t column_index);
    int64_t tsfile_result_set_get_value_by_index_int64_t(ResultSet result_set, uint32_t column_index);
    bint tsfile_result_set_get_value_by_index_bool(ResultSet result_set, uint32_t column_index);
    float tsfile_result_set_get_value_by_index_float(ResultSet result_set, uint32_t column_index);
    double tsfile_result_set_get_value_by_index_double(ResultSet result_set, uint32_t column_index);
    ResultSetMetaData tsfile_result_set_get_metadata(ResultSet result_set);
