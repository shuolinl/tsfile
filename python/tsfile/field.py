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

import numpy as np
import pandas as pd

from .constants import TSDataType
from .date_utils import parse_int_to_date


class Field(object):
    def __init__(self, field_name, data_type, value=None):
        """
        :param data_type: TSDataType
        """
        self.__data_type = data_type
        self.value = value
        if not isinstance(value, data_type.to_py_type()):
            raise TypeError(f"Expected {data_type.to_py_type()} got {type(value)}")
        self.field_name = field_name

    def get_data_type(self):
        return self.__data_type

    def get_field_name(self):
        return self.field_name

    def is_null(self):
        return self.__data_type is None or self.value is None or self.value is pd.NA

    def set_bool_value(self, value: bool):
        self.value = value

    def get_bool_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        if (
                self.__data_type != TSDataType.BOOLEAN
                or self.value is None
                or self.value is pd.NA
        ):
            return None
        return self.value

    def set_int_value(self, value: int):
        if not isinstance(value, int):
            raise TypeError(f"Expected int got {type(value)}")
        if not np.iinfo(np.int32).min <= value <= np.iinfo(np.int32).max:
            raise OverflowError(f"data:{value} out of range of int32")
        self.value = value

    def get_int_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")

        if (
                self.__data_type != TSDataType.INT32
                and self.__data_type != TSDataType.DATE
                or self.value is None
                or self.value is pd.NA
        ):
            return None
        return np.int32(self.value)

    def set_long_value(self, value: int):
        if not isinstance(value, int):
            raise TypeError(f"Expected int got {type(value)}")

        if not np.iinfo(np.int64).min <= value <= np.iinfo(np.int64).max:
            raise OverflowError(f"data:{value} out of range of int64")
        self.value = value

    def get_long_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        if (
                self.__data_type != TSDataType.INT64
                and self.__data_type != TSDataType.TIMESTAMP
                and self.__data_type != TSDataType.INT32
                or self.value is None
                or self.value is pd.NA
        ):
            return None
        return np.int64(self.value)

    def set_float_value(self, value: float):
        if isinstance(value, float):
            raise TypeError(f"Expected float got {type(value)}")
        if not np.finfo(np.float32).min <= value <= np.finfo(np.float32).max:
            raise OverflowError(f"data:{value} out of range of float32")
        self.value = value

    def get_float_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        if (
                self.__data_type != TSDataType.FLOAT
                or self.value is None
                or self.value is pd.NA
        ):
            return None
        return np.float32(self.value)

    def set_double_value(self, value: float):
        if isinstance(value, float):
            raise TypeError(f"Expected float got {type(value)}")
        if not np.finfo(np.float64).min <= value <= np.finfo(np.float64).max:
            raise OverflowError(f"data:{value} out of range of float64")
        self.value = value

    def get_double_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        if (
                self.__data_type != TSDataType.DOUBLE
                or self.value is None
                or self.value is pd.NA
        ):
            return None
        return np.float64(self.value)

    def set_binary_value(self, value: bytes):
        self.value = value

    def get_binary_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        if (
                self.__data_type != TSDataType.TEXT
                and self.__data_type != TSDataType.STRING
                and self.__data_type != TSDataType.BLOB
                or self.value is None
                or self.value is pd.NA
        ):
            return None
        return self.value

    def get_date_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        if (
                self.__data_type != TSDataType.DATE
                or self.value is None
                or self.value is pd.NA
        ):
            return None
        return parse_int_to_date(self.value)

    def get_string_value(self):
        if self.__data_type is None or self.value is None or self.value is pd.NA:
            return "None"
        # TEXT, STRING
        elif self.__data_type == TSDataType.TEXT or self.__data_type == TSDataType.STRING:
            return self.value.decode("utf-8")
        # BLOB
        elif self.__data_type == TSDataType.BLOB:
            return str(hex(int.from_bytes(self.value, byteorder="big")))
        else:
            return str(self.get_object_value(self.__data_type))

    def __str__(self):
        return self.get_string_value()

    def get_object_value(self, data_type: TSDataType):
        """
        :param data_type: TSDataType
        """
        if self.__data_type is None or self.value is None or self.value is pd.NA:
            return None
        if data_type == TSDataType.BOOLEAN:
            return bool(self.value)
        elif data_type == TSDataType.INT32:
            return np.int32(self.value)
        elif data_type == TSDataType.INT64 or data_type == TSDataType.TIMESTAMP:
            return np.int64(self.value)
        elif data_type == TSDataType.FLOAT:
            return np.float32(self.value)
        elif data_type == TSDataType.DOUBLE:
            return np.float64(self.value)
        elif data_type == TSDataType.DATE:
            return parse_int_to_date(self.value)
        elif data_type == TSDataType.TEXT or data_type == TSDataType.BLOB or data_type == TSDataType.STRING:
            return self.value
        else:
            raise RuntimeError("Unsupported data type:" + str(data_type))

    @staticmethod
    def get_field(field_name, value, data_type):
        """
        :param field_name: field's name
        :param value: field value corresponding to the data type
        :param data_type: TSDataType
        """
        if value is None or value is pd.NA:
            return None
        field = Field(field_name, data_type, value)
        return field
