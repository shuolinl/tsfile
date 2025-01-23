/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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
#include <gtest/gtest.h>
#include <unistd.h>
extern "C" {
#include "cwrapper/tsfile_cwrapper.h"
}

#include "utils/errno_define.h"

using namespace common;

namespace cwrapper {
#define TSFILE_NAME "cwrapper.tsfile"
class CWrapperTest : public testing::Test {
   protected:
    void TearDown() override { remove(TSFILE_NAME); }
};

TEST_F(CWrapperTest, RegisterTimeSeries) {
    ERRNO code = 0;
    char* temperature = strdup("temperature");
    TimeseriesSchema ts_schema{temperature, TS_DATATYPE_INT32,
                               TS_ENCODING_PLAIN, TS_COMPRESSION_UNCOMPRESSED};
    TsFileWriter writer = tsfile_writer_new(TSFILE_NAME, &code);
    ASSERT_EQ(code, 0);
    code = tsfile_writer_register_timeseries(writer, "device1", &ts_schema);
    ASSERT_EQ(code, 0);
    free(temperature);
}

TEST_F(CWrapperTest, WriterFlushTabletAndReadData) {
    ERRNO code = 0;
    const int device_num = 50;
    const int measurement_num = 50;
    DeviceSchema device_schema[50];
    TsFileWriter writer = tsfile_writer_new(TSFILE_NAME, &code);
    ASSERT_EQ(code, 0);
    for (int i = 0; i < device_num; i++) {
        char* device_name = strdup(("device" + std::to_string(i)).c_str());
        device_schema[i].device_name = device_name;
        device_schema[i].timeseries_num = measurement_num;
        device_schema[i].timeseries_schema = (TimeseriesSchema*)malloc(
            sizeof(TimeseriesSchema) * measurement_num);
        for (int j = 0; j < measurement_num; j++) {
            TimeseriesSchema* schema = device_schema[i].timeseries_schema + j;
            schema->name = strdup(("measurement" + std::to_string(j)).c_str());
            schema->compression = TS_COMPRESSION_UNCOMPRESSED;
            schema->data_type = TS_DATATYPE_INT64;
            schema->encoding = TS_ENCODING_PLAIN;
        }
        code = tsfile_writer_register_device(writer, &device_schema[i]);
        ASSERT_EQ(code, 0);
    }
    int max_rows = 100;
    for (int i = 0; i < device_num; i++) {
        char* device_name = strdup(("device" + std::to_string(i)).c_str());
        char** measurements_name = static_cast<char**>(malloc(measurement_num * sizeof(char*)));
        TSDataType *data_types = static_cast<TSDataType*>(
            malloc(sizeof(TSDataType) * measurement_num));
        for (int j = 0; j < measurement_num; j++) {
            measurements_name[j] = strdup(("measurement" + std::to_string(j)).c_str());
            data_types[j] = TS_DATATYPE_INT64;
        }
        Tablet tablet = tablet_new_with_device(device_name, measurements_name, data_types, measurement_num, max_rows);
        free(device_name);
        free(data_types);
        for (int j = 0; j < measurement_num; j++) {
            free(measurements_name[j]);
        }
        free(measurements_name);
        for (int j = 0; j < measurement_num; j++) {
            for (int row = 0; row < max_rows; row++) {
                tablet_add_timestamp(tablet, row, 16225600 + row);
            }
            for (int row = 0; row < max_rows; row++) {
                tablet_add_value_by_index_int64_t(tablet, row, j, static_cast<int64_t>(row));
            }
        }
        code = tsfile_writer_write_tablet(writer, tablet);
        ASSERT_EQ(code, 0);
        code = destroy_tablet(tablet);
    }
    ASSERT_EQ(tsfile_writer_flush_data(writer), 0);
    ASSERT_EQ(tsfile_writer_close(writer), 0);

    TsFileReader reader = tsfile_reader_new(TSFILE_NAME, &code);
    ASSERT_EQ(code, 0);

    char** select_list = static_cast<char**>(malloc(measurement_num * sizeof(char*)));
    for (int i = 0; i < measurement_num; i++) {
        select_list[i] = strdup(("device" + std::to_string(i) + ".measurement" + std::to_string(i)).c_str());
    }
    ResultSet result_set =  tsfile_reader_query_path(reader, select_list, measurement_num, 16225600, 16225600 + max_rows -1);

    ResultSetMetaData metadata = tsfile_result_set_get_metadata(result_set);
    ASSERT_EQ(metadata.column_num, measurement_num);
    ASSERT_EQ(std::string(metadata.column_names[4]), std::string("device4.measurement4"));
    ASSERT_EQ(metadata.data_types[9], TS_DATATYPE_INT64);
    for (int i = 0; i < measurement_num - 1; i++) {
        ASSERT_TRUE(tsfile_result_set_has_next(result_set));
        ASSERT_FALSE(tsfile_result_set_is_null_by_index(result_set,i));
        ASSERT_EQ(tsfile_result_set_get_value_by_index_int64_t(result_set, i), i);
        ASSERT_EQ(tsfile_result_set_get_value_by_name_int64_t(result_set, std::string("measurement" + std::to_string(i)).c_str()), i);
    }












}

}  // namespace cwrapper