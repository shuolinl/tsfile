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

class LibraryError(Exception):
    _default_message = "Unknown error occurred"
    _default_code = -1

    def __init__(self, code=None, message=None):
        self.code = code
        self.message = message or self._default_message
        super().__init__(f"[{code}] {self.message}")

class OOMError(LibraryError):
    default_message = "Out of memory"
    default_code = 1

class NotExistsError(LibraryError):
    default_message = "Requested resource does not exist"
    default_code = 2

class AlreadyExistsError(LibraryError):
    default_message = "Resource already exists"
    default_code = 3

# 参数相关异常
class InvalidArgumentError(LibraryError):
    default_message = "Invalid argument provided"
    default_code = 4

class OutOfRangeError(LibraryError):
    default_message = "Value out of valid range"
    default_code = 5

# IO操作相关异常
class PartialReadError(LibraryError):
    default_message = "Incomplete data read operation"
    default_code = 6

class FileOpenError(LibraryError):
    default_message = "Failed to open file"
    default_code = 28

class FileCloseError(LibraryError):
    default_message = "Failed to close file"
    default_code = 29

class FileWriteError(LibraryError):
    default_message = "Failed to write to file"
    default_code = 30

class FileReadError(LibraryError):
    default_message = "Failed to read from file"
    default_code = 31

class FileSyncError(LibraryError):
    default_message = "Failed to sync file contents"
    default_code = 32

class MetadataError(LibraryError):
    default_message = "Metadata inconsistency detected"
    default_code = 33

class BufferNotEnoughError(LibraryError):
    default_message = "Insufficient buffer space"
    default_code = 36

class DeviceNotExistError(LibraryError):
    default_message = "Requested device does not exist"
    default_code = 44

class MeasurementNotExistError(LibraryError):
    default_message = "Specified measurement does not exist"
    default_code = 45

class InvalidQueryError(LibraryError):
    default_message = "Malformed query syntax"
    default_code = 46

class CompressionError(LibraryError):
    default_message = "Data compression/decompression failed"
    default_code = 48

class TableNotExistError(LibraryError):
    default_message = "Requested table does not exist"
    default_code = 49


class TypeNotSupportedError(LibraryError):
    default_message = "Unsupported data type"
    default_code = 26

class TypeMismatchError(LibraryError):
    default_message = "Data type mismatch"
    default_code = 27

ERROR_MAPPING = {
    1: OOMError,
    2: NotExistsError,
    3: AlreadyExistsError,
    4: InvalidArgumentError,
    5: OutOfRangeError,
    6: PartialReadError,
    26: TypeNotSupportedError,
    27: TypeMismatchError,
    28: FileOpenError,
    29: FileCloseError,
    30: FileWriteError,
    31: FileReadError,
    32: FileSyncError,
    33: MetadataError,
    36: BufferNotEnoughError,
    44: DeviceNotExistError,
    45: MeasurementNotExistError,
    46: InvalidQueryError,
    48: CompressionError,
    49: TableNotExistError,
}

def get_exception(code : int, context: str = "") -> BaseException:
    if code == 0:
        return None

    exc_type = ERROR_MAPPING.get(code, BaseException)
    return exc_type(code, message=context)
