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

#include "tsfile_cwrapper_tree.h"

#include <iomanip>

#include "common/global.h"
#include "reader/expression.h"
#include "reader/filter/and_filter.h"
#include "reader/filter/filter.h"
#include "reader/filter/time_filter.h"
#include "reader/filter/time_operator.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "utils/errno_define.h"
#include "writer/tsfile_writer.h"

#define CONSTRUCT_EXP_INTERNAL(exp, column_name) \
    do {                                         \
        exp.column_name = column_name;           \
        exp.operatype = oper;                    \
        exp.children_length = 0;                 \
    } while (0)

#define INSERT_DATA_TABLET_STEP                                            \
    do {                                                                   \
        for (int i = 0; i < tablet->column_num; i++) {                     \
            if (strcmp(tablet->timeseries_schema[i]->name, column_name) == \
                0) {                                                       \
                column_id = i;                                             \
                break;                                                     \
            }                                                              \
        }                                                                  \
        if (column_id == -1) {                                             \
            return tablet;                                                 \
        }                                                                  \
        if (tablet->cur_num + 1 > tablet->max_capacity) {                  \
            return tablet;                                                 \
        }                                                                  \
        tablet->times[line_id] = timestamp;                                \
    } while (0)

#define DataType common::TSDataType
#define Encoding common::TSEncoding
#define CompressionType common::CompressionType
#define E_OK common::E_OK
#define TsRecord storage::TsRecord
#define DataPoint storage::DataPoint
#define E_BUF_NOT_ENOUGH common::E_BUF_NOT_ENOUGH
Expression create_column_filter(const char* column_name, OperatorType oper,
                                int32_t int32_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = int32_value;
    exp.const_condition.type = TSDataType::TS_DATATYPE_INT32;
    return exp;
}

Expression create_column_filter(const char* column_name, OperatorType oper,
                                int64_t int64_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = int64_value;
    exp.const_condition.type = TSDataType::TS_DATATYPE_INT64;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                bool bool_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = bool_value ? 1 : 0;
    exp.const_condition.type = TSDataType::TS_DATATYPE_BOOLEAN;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                float float_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    memcpy(&exp.const_condition.value_condition, &float_value, sizeof(float));
    exp.const_condition.type = TSDataType::TS_DATATYPE_FLOAT;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                double double_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = double_value;
    exp.const_condition.type = TSDataType::TS_DATATYPE_DOUBLE;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                const char* char_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = reinterpret_cast<int64_t>(char_value);
    exp.const_condition.type = TSDataType::TS_DATATYPE_TEXT;
    return exp;
}

TimeFilterExpression* create_andquery_timefilter() {
    storage::Expression* exp = new storage::Expression(storage::AND_EXPR);
    return (TimeFilterExpression*)exp;
}

TimeFilterExpression* create_time_filter(const char* table_name,
                                         const char* column_name,
                                         OperatorType oper, int64_t timestamp) {
    std::string table_name_str(table_name);
    std::string column_name_str(column_name);
    storage::Path path(table_name_str, column_name_str);
    storage::Filter* filter;
    switch (oper) {
        case GT:
            filter = storage::TimeFilter::gt(timestamp);
            break;
        case LT:
            filter = storage::TimeFilter::lt(timestamp);
            break;
        case EQ:
            filter = storage::TimeFilter::eq(timestamp);
            break;
        case NOTEQ:
            filter = storage::TimeFilter::not_eqt(timestamp);
            break;
        case GE:
            filter = storage::TimeFilter::gt_eq(timestamp);
            break;
        case LE:
            filter = storage::TimeFilter::lt_eq(timestamp);
            break;
        default:
            filter = nullptr;
            break;
    }
    storage::Expression* exp =
        new storage::Expression(storage::SERIES_EXPR, path, filter);
    return (TimeFilterExpression*)exp;
}

TimeFilterExpression* add_time_filter_to_and_query(
    TimeFilterExpression* exp_and, TimeFilterExpression* exp) {
    storage::Expression* and_exp = (storage::Expression*)exp_and;
    storage::Expression* time_exp = (storage::Expression*)exp;
    if (and_exp->left_ == nullptr) {
        and_exp->left_ = time_exp;
    } else if (and_exp->right_ == nullptr) {
        and_exp->right_ = time_exp;
    } else {
        storage::Expression* new_exp =
            new storage::Expression(storage::AND_EXPR);
        new_exp->left_ = and_exp->right_;
        and_exp->right_ = new_exp;
        add_time_filter_to_and_query((TimeFilterExpression*)new_exp, exp);
    }
    return exp_and;
}

void destory_time_filter_query(TimeFilterExpression* expression) {
    if (expression == nullptr) {
        return;
    }

    destory_time_filter_query(
        (TimeFilterExpression*)((storage::Expression*)expression)->left_);
    destory_time_filter_query(
        (TimeFilterExpression*)((storage::Expression*)expression)->right_);
    storage::Expression* exp = (storage::Expression*)expression;
    if (exp->type_ == storage::ExpressionType::SERIES_EXPR) {
        delete exp->filter_;
    } else {
        delete exp;
    }
}

Expression create_global_time_expression(OperatorType oper, int64_t timestamp) {
    Expression exp;
    exp.operatype = oper;
    exp.expression_type = GLOBALTIME;
    exp.const_condition.value_condition = timestamp;
    exp.const_condition.type = TSDataType::TS_DATATYPE_INT64;
    return exp;
}

Expression* and_filter_to_and_query(Expression* exp_and, Expression* exp) {
    if (exp_and->children_length >= MAX_COLUMN_FILTER_NUM - 1) {
        return nullptr;
    }
    exp_and->children[exp_and->children_length++] = exp;
    return exp_and;
}

QueryDataRet ts_reader_query(TsFileReader reader, const char* table_name,
                             const char** columns_name, int column_num,
                             TimeFilterExpression* expression) {
    auto* r = (storage::TsFileReader*)reader;
    std::string table_name_str(table_name);
    std::vector<storage::Path> selected_paths;
    for (int i = 0; i < column_num; i++) {
        std::string column_name(columns_name[i]);
        selected_paths.push_back(storage::Path(table_name_str, column_name));
    }

    storage::ResultSet* qds = nullptr;
    storage::QueryExpression* query_expression =
        storage::QueryExpression::create(selected_paths,
                                         (storage::Expression*)expression);
    r->query(query_expression, qds);
    QueryDataRet ret = (QueryDataRet)malloc(sizeof(struct query_data_ret));
    ret->data = qds;
    ret->column_names = (char**)malloc(column_num * sizeof(char*));
    ret->column_num = column_num;
    for (int i = 0; i < column_num; i++) {
        ret->column_names[i] = strdup(columns_name[i]);
    }
    return ret;
}

ERRNO destory_query_dataret(QueryDataRet data) {
    storage::ResultSet* qds = (storage::ResultSet*)data->data;
    delete qds;
    for (int i = 0; i < data->column_num; i++) {
        free(data->column_names[i]);
    }
    free(data->column_names);
    free(data);
    return E_OK;
}