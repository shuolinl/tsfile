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

#include "tsfile_cwrapper.h"

#include <reader/result_set.h>

#include <iomanip>

#include "common/global.h"
#include "common/tablet.h"
#include "reader/expression.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"
static bool is_init = false;

Tablet tablet_new(const char *device_id, const char **column_name_list,
                  TSDataType *data_types, uint32_t column_num) {
    std::vector<std::string> measurement_list;
    std::vector<common::TSDataType> data_type_list;
    for (int i = 0; i < column_num; i++) {
        measurement_list.emplace_back(column_name_list[i]);
        data_type_list.push_back(
            static_cast<common::TSDataType>(*(data_types + i)));
    }
    auto *tablet =
        new storage::Tablet(device_id, &measurement_list, &data_type_list);
    return tablet;
}

Tablet tablet_new(const char **column_name_list, TSDataType *data_types,
                  uint32_t column_num) {
    std::vector<std::string> measurement_list;
    std::vector<common::TSDataType> data_type_list;
    for (int i = 0; i < column_num; i++) {
        measurement_list.emplace_back(column_name_list[i]);
        data_type_list.push_back(
            static_cast<common::TSDataType>(*(data_types + i)));
    }
    auto *tablet = new storage::Tablet("", &measurement_list, &data_type_list);
    return tablet;
}

uint32_t tablet_get_cur_row_size(Tablet tablet) {
    return static_cast<storage::Tablet *>(tablet)->get_cur_row_size();
}

ERRNO tablet_add_timestamp(Tablet tablet, uint32_t row_index,
                           timestamp timestamp) {
    return static_cast<storage::Tablet *>(tablet)->add_timestamp(row_index,
                                                                 timestamp);
}

#define tablet_add_value_by_name_def(type)                                   \
    ERRNO tablet_add_value_by_name_##type(Tablet tablet, uint32_t row_index, \
                                          const char *column_name,           \
                                          type value) {                      \
        return static_cast<storage::Tablet *>(tablet)->add_value(            \
            row_index, column_name, value);                                  \
    }

tablet_add_value_by_name_def(int32_t);
tablet_add_value_by_name_def(int64_t);
tablet_add_value_by_name_def(float);
tablet_add_value_by_name_def(double);
tablet_add_value_by_name_def(bool);

#define table_add_value_by_index_def(type)                                    \
    ERRNO tablet_add_value_by_index_##type(Tablet tablet, uint32_t row_index, \
                                           uint32_t column_index,             \
                                           type value) {                      \
        return static_cast<storage::Tablet *>(tablet)->add_value(             \
            row_index, column_index, value);                                  \
    }

table_add_value_by_index_def(int32_t);
table_add_value_by_index_def(int64_t);
table_add_value_by_index_def(float);
table_add_value_by_index_def(double);
table_add_value_by_index_def(bool);

void *tablet_get_value(Tablet tablet, uint32_t row_index, uint32_t schema_index,
                       TSDataType *type) {
    common::TSDataType data_type;
    void *result = static_cast<storage::Tablet *>(tablet)->get_value(
        row_index, schema_index, data_type);
    *type = static_cast<TSDataType>(data_type);
    return result;
}

// TsRecord API
TsRecord ts_record_new(const char *device_name, int64_t timestamp,
                       int timeseries_num) {
    auto *record =
        new storage::TsRecord(timestamp, device_name, timeseries_num);
    return record;
}

#define insert_data_into_ts_record_by_name_def(type)                 \
    ERRNO insert_data_into_ts_record_by_name##type(                  \
        TsRecord data, const char *measurement_name, type value) {   \
        auto *record = (storage::TsRecord *)data;                    \
        storage::DataPoint point(measurement_name, value);           \
        if (record->points_.size() + 1 > record->points_.capacity()) \
            return common::E_BUF_NOT_ENOUGH;                         \
        record->points_.push_back(point);                            \
        return common::E_OK;                                         \
    }

insert_data_into_ts_record_by_name_def(int32_t);
insert_data_into_ts_record_by_name_def(int64_t);
insert_data_into_ts_record_by_name_def(bool);
insert_data_into_ts_record_by_name_def(float);
insert_data_into_ts_record_by_name_def(double);

void init_tsfile_config() {
    if (!is_init) {
        common::init_config_value();
        is_init = true;
    }
}

TsFileReader tsfile_reader_open(const char *pathname, ERRNO *err_code) {
    init_tsfile_config();
    auto reader = new storage::TsFileReader();
    int ret = reader->open(pathname);
    if (ret != common::E_OK) {
        std::cout << "open file failed" << std::endl;
        *err_code = ret;
        delete reader;
        return nullptr;
    }
    return reader;
}

TsFileWriter tsfile_writer_open(const char *pathname, ERRNO *err_code) {
    init_tsfile_config();
    auto writer = new storage::TsFileWriter();
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    int ret = writer->open(pathname, flags, 0644);
    if (ret != common::E_OK) {
        delete writer;
        *err_code = ret;
        return nullptr;
    }
    return writer;
}

TsFileWriter tsfile_writer_open_flag(const char *pathname, mode_t flag,
                                     ERRNO *err_code) {
    init_tsfile_config();
    auto *writer = new storage::TsFileWriter();
    int ret = writer->open(pathname, O_CREAT | O_RDWR, flag);
    if (ret != common::E_OK) {
        delete writer;
        *err_code = ret;
        return nullptr;
    }
    return writer;
}

ERRNO tsfile_writer_close(TsFileWriter writer) {
    auto *w = static_cast<storage::TsFileWriter *>(writer);
    int ret = w->close();
    delete w;
    return ret;
}

ERRNO tsfile_reader_close(TsFileReader reader) {
    auto *ts_reader = (storage::TsFileReader *)reader;
    delete ts_reader;
    return common::E_OK;
}

void tsfile_writer_register_table(TsFileWriter writer, TableSchema *schema) {
    std::vector<storage::MeasurementSchema *> measurement_schemas;
    std::vector<storage::ColumnCategory> column_categories;
    measurement_schemas.resize(schema->column_num);
    for (int i = 0; i < schema->column_num; i++) {
        ColumnSchema *cur_schema = schema->column_schemas + i;
        measurement_schemas[i] = new storage::MeasurementSchema(cur_schema->column_name,
            static_cast<common::TSDataType>(cur_schema->data_type));
        column_categories.push_back(
            static_cast<storage::ColumnCategory>(cur_schema->column_category));
    }
    auto tsfile_writer = static_cast<storage::TsFileWriter *>(writer);
    tsfile_writer->register_table(std::make_shared<storage::TableSchema>(
        schema->table_name, measurement_schemas, column_categories));
}

ERRNO tsfile_register_timeseries(TsFileWriter writer, const char *device_name,
                                 TimeseriesSchema *schema) {
    auto *w = (storage::TsFileWriter *)writer;

    int ret = w->register_timeseries(
        device_name, storage::MeasurementSchema(
                         schema->name, (common::TSDataType)schema->data_type,
                         (common::TSEncoding)schema->encoding,
                         (common::CompressionType)schema->compression));
    return ret;
}

ERRNO tsfile_register_device(TsFileWriter writer,
                             device_schema *device_schema) {
    storage::TsFileWriter *w = (storage::TsFileWriter *)writer;
    for (int column_id = 0; column_id < device_schema->timeseries_num;
         column_id++) {
        TimeseriesSchema *schema = device_schema->timeseries_schema[column_id];
        ERRNO ret = w->register_timeseries(
            device_schema->device_name,
            storage::MeasurementSchema(
                schema->name, (common::TSDataType)schema->data_type,
                (common::TSEncoding)schema->encoding,
                (common::CompressionType)schema->compression));
        if (ret != common::E_OK) {
            return ret;
        }
    }
    return common::E_OK;
}

ERRNO tsfile_writer_write_ts_record(TsFileWriter writer, TsRecord data) {
    auto *w = static_cast<storage::TsFileWriter *>(writer);
    const auto *record = static_cast<storage::TsRecord *>(data);
    const int ret = w->write_record(*record);
    if (ret == common::E_OK) {
        delete record;
    }
    return ret;
}

ERRNO tsfile_writer_write_tablet(TsFileWriter writer, Tablet tablet) {
    auto *w = static_cast<storage::TsFileWriter *>(writer);
    const auto *tbl = static_cast<storage::Tablet *>(tablet);
    return w->write_tablet(*tbl);
}

ERRNO tsfile_writer_flush_data(TsFileWriter writer) {
    auto *w = static_cast<storage::TsFileWriter *>(writer);
    return w->flush();
}

// Query

ResultSet tsfile_reader_query_table(TsFileReader reader, char *table_name,
                                    char **columns, uint32_t column_num,
                                    timestamp start_time, timestamp end_time) {
    auto *r = static_cast<storage::TsFileReader *>(reader);
    std::string table_name_str(table_name);
    std::vector<std::string> selected_paths;
    for (int i = 0; i < column_num; i++) {
        std::string column_name(columns[i]);
        selected_paths.push_back(table_name_str + "." + column_name);
    }
    storage::ResultSet *qds = nullptr;
    r->query(selected_paths, start_time, end_time, qds);
    return qds;
}

ResultSet tsfile_reader_query_path(TsFileReader reader, char **path_list,
                                   uint32_t path_num, timestamp start_time,
                                   timestamp end_time) {
    auto *r = static_cast<storage::TsFileReader *>(reader);
    std::vector<std::string> selected_paths;
    for (int i = 0; i < path_num; i++) {
        selected_paths.push_back(path_list[i]);
    }
    storage::ResultSet *qds = nullptr;
    r->query(selected_paths, start_time, end_time, qds);
    return qds;
}

#define tsfile_result_set_get_value_by_name_def(type)                         \
    type tsfile_result_set_get_value_by_name##type(ResultSet result_set,      \
                                                   const char *column_name) { \
        auto *r = static_cast<storage::ResultSet *>(result_set);              \
        return r->get_value<type>(column_name);                               \
    }
tsfile_result_set_get_value_by_name_def(bool);
tsfile_result_set_get_value_by_name_def(int32_t);
tsfile_result_set_get_value_by_name_def(int64_t);
tsfile_result_set_get_value_by_name_def(float);
tsfile_result_set_get_value_by_name_def(double);

#define tsfile_result_set_get_value_by_index_def(type)                        \
    type tsfile_result_set_get_value_by_index_##type(ResultSet result_set,    \
                                                     uint32_t column_index) { \
        auto *r = static_cast<storage::ResultSet *>(result_set);              \
        return r->get_value<type>(column_index);                              \
    }

tsfile_result_set_get_value_by_index_def(int32_t);
tsfile_result_set_get_value_by_index_def(int64_t);
tsfile_result_set_get_value_by_index_def(float);
tsfile_result_set_get_value_by_index_def(double);
tsfile_result_set_get_value_by_index_def(bool);

#define tsfile_result_set_is_null_by_name_def(type)                          \
    bool tsfile_result_set_is_null_by_name_##type(ResultSet result_set,      \
                                                  const char *column_name) { \
        auto *r = static_cast<storage::ResultSet *>(result_set);             \
        return r->is_null(column_name);                                      \
    }

tsfile_result_set_is_null_by_name_def(bool);
tsfile_result_set_is_null_by_name_def(int32_t);
tsfile_result_set_is_null_by_name_def(int64_t);
tsfile_result_set_is_null_by_name_def(float);
tsfile_result_set_is_null_by_name_def(double);

#define tsfile_result_set_is_null_by_index_def(type)                        \
    bool tsfile_result_set_is_null_by_index_##type(ResultSet result_set,    \
                                                   uint32_t column_index) { \
        auto *r = static_cast<storage::ResultSet *>(result_set);            \
        return r->is_null(column_index);                                    \
    }

tsfile_result_set_is_null_by_index_def(bool);
tsfile_result_set_is_null_by_index_def(int32_t);
tsfile_result_set_is_null_by_index_def(int64_t);
tsfile_result_set_is_null_by_index_def(float);
tsfile_result_set_is_null_by_index_def(double);

ResultSetMetaData tsfile_result_set_get_metadata(ResultSet result_set) {
    auto *r = static_cast<storage::ResultSet *>(result_set);
    ResultSetMetaData meta_data;
    storage::ResultSetMetadata *result_set_metadata = r->get_metadata();
    meta_data.column_num = result_set_metadata->get_column_count();
    meta_data.column_names =
        static_cast<char **>(malloc(meta_data.column_num * sizeof(char *)));
    meta_data.data_types = static_cast<TSDataType *>(
        malloc(meta_data.column_num * sizeof(TSDataType)));
    for (int i = 0; i < meta_data.column_num; i++) {
        meta_data.column_names[i] =
            strdup(result_set_metadata->get_column_name(i).c_str());
        meta_data.data_types[i] =
            static_cast<TSDataType>(result_set_metadata->get_column_type(i));
    }
    return meta_data;
}

char *tsfile_result_set_meta_get_column_name(ResultSet result_set,
                                             uint32_t column_index) {
    auto *r = static_cast<storage::ResultSet *>(result_set);
    return strdup(r->get_metadata()->get_column_name(column_index).c_str());
}

TSDataType tsfile_result_set_meta_get_data_type(ResultSet result_set,
                                                uint32_t column_index) {
    auto *r = static_cast<storage::ResultSet *>(result_set);
    return static_cast<TSDataType>(
        r->get_metadata()->get_column_type(column_index));
}

uint32_t tsfile_result_set_meta_get_column_num(ResultSet result_set) {
    auto *r = static_cast<storage::ResultSet *>(result_set);
    return r->get_metadata()->get_column_count();
}

TableSchema tsfile_reader_get_table_schema(TsFileReader reader,
                                           const char *table_name) {
    auto *r = static_cast<storage::TsFileReader *>(reader);
    std::vector<storage::MeasurementSchema> schemas;
    r->get_timeseries_schema(
        std::make_shared<storage::StringArrayDeviceID>(table_name), schemas);
    TableSchema schema;
    schema.table_name = strdup(table_name);
    schema.column_num = schemas.size();
    schema.column_schemas = static_cast<ColumnSchema *>(
        malloc(sizeof(ColumnSchema) * schema.column_num));

    for (uint32_t i = 0; i < schemas.size(); i++) {
        schema.column_schemas[i].column_category = FIELD;
        schema.column_schemas[i].column_name =
            strdup(schemas[i].measurement_name_.c_str());
        schema.column_schemas[i].data_type =
            static_cast<TSDataType>(schemas[i].data_type_);
    }
    return schema;
}

TableSchema *tsfile_reader_get_all_table_schemas(TsFileReader reader,
                                                 const char *table_name,
                                                 uint32_t *num) {
    auto *r = static_cast<storage::TsFileReader *>(reader);
    std::vector<std::shared_ptr<storage::IDeviceID>> devices =
        r->get_all_devices(table_name);
    *num = devices.size();
    TableSchema *schemas = static_cast<TableSchema *>(
        malloc(sizeof(TableSchema) * devices.size()));
    std::vector<storage::MeasurementSchema> measurement_schemas;
    for (uint32_t i = 0; i < devices.size(); i++) {
        r->get_timeseries_schema(devices[i], measurement_schemas);
        schemas[i].table_name =
            strdup(devices[i].get()->get_table_name().c_str());
        schemas[i].column_num = measurement_schemas.size();
        for (int j = 0; j < measurement_schemas.size(); j++) {
            schemas[i].column_schemas[j].column_category = FIELD;
            schemas[i].column_schemas[j].column_name =
                strdup(measurement_schemas[j].measurement_name_.c_str());
            schemas[i].column_schemas[j].data_type =
                static_cast<TSDataType>(measurement_schemas[j].data_type_);
        }
    }
    return schemas;
}

// delete pointer
ERRNO delete_tsfile_ts_record(TsRecord record) {
    auto *r = static_cast<storage::TsRecord *>(record);
    if (r != nullptr) {
        delete r;
    }
    return common::E_OK;
}
