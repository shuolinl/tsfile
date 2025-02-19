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

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
from Cython.Build import cythonize
import numpy as np
import platform
import shutil
import os

version = "2.1.0.dev"
system = platform.system()

def copy_tsfile_lib(source_dir, target_dir, suffix):
    lib_file_name = f"libtsfile.{suffix}"
    source = os.path.join(source_dir, lib_file_name)
    target = os.path.join(target_dir, lib_file_name)

    if os.path.exists(source):
        shutil.copyfile(source, target)

    link_name = os.path.join(target_dir, "libtsfile.dylib")
    if os.path.exists(link_name):
        os.remove(link_name)

    if system == "Linux":
        os.symlink(lib_file_name, link_name)
    elif system == "Darwin":
        os.symlink(lib_file_name, link_name)


def copy_tsfile_header(source, target):
    if os.path.exists(source):
        shutil.copyfile(source, target)

project_dir = os.path.dirname(os.path.abspath(__file__))

## Copy C wrapper header.
# tsfile/cpp/src/cwrapper/tsfile_cwrapper.h
tsfile_c_include_file = os.path.join(
    project_dir, "..", "cpp", "src", "cwrapper", "tsfile_cwrapper.h"
)
tsfile_py_include_file = os.path.join(project_dir, "tsfile", "tsfile_cwrapper.h")
copy_tsfile_header(tsfile_c_include_file, tsfile_py_include_file)

## Copy shared library
tsfile_shared_source_dir = os.path.join(project_dir, "..", "cpp", "target", "build", "lib")
tsfile_shared_dir = os.path.join(project_dir, "tsfile")

if system == "Darwin":
    copy_tsfile_lib(tsfile_shared_source_dir, tsfile_shared_dir, version + ".dylib")
elif system == "Linux":
    copy_tsfile_lib(tsfile_shared_source_dir, tsfile_shared_dir, "so." + version)
else:
    copy_tsfile_lib(tsfile_shared_source_dir, tsfile_shared_dir, "dll")

tsfile_include_dir=os.path.join(project_dir, "tsfile")


ext_modules_tsfile = [
    # utils: from python to c or c to python.
    Extension(
        "tsfile.tsfile_py_cpp",
        sources=[os.path.join("tsfile", "tsfile_py_cpp.pyx")],
        libraries=["tsfile"],
        library_dirs=[tsfile_shared_dir],
        include_dirs=[tsfile_include_dir, np.get_include()],
        runtime_library_dirs=[tsfile_shared_dir] if system != "Windows" else None,
        extra_compile_args=(
            ["-std=c++11"] if system != "Windows" else ["-std=c++11", "-DMS_WIN64"]
        ),
        language="c++",
    ),
    # query data and describe schema: tsfile reader module
    Extension(
        "tsfile.tsfile_reader",
        sources=[os.path.join("tsfile", "tsfile_reader.pyx")],
        libraries=["tsfile"],
        library_dirs=[tsfile_shared_dir],
        depends=[os.path.join("tsfile", "tsfile_py_cpp.pxd")],
        include_dirs=[tsfile_include_dir, np.get_include()],
        runtime_library_dirs=[tsfile_shared_dir] if system != "Windows" else None,
        extra_compile_args=(
            ["-std=c++11"] if system != "Windows" else ["-std=c++11", "-DMS_WIN64"]
        ),
        language="c++",
    ),
    # write data and register schema: tsfile writer module
    Extension(
        "tsfile.tsfile_writer",
        sources=[os.path.join("tsfile", "tsfile_writer.pyx")],
        libraries=["tsfile"],
        library_dirs=[tsfile_shared_dir],
        depends=[os.path.join("tsfile", "tsfile_py_cpp.pxd")],
        include_dirs=[tsfile_include_dir, np.get_include()],
        runtime_library_dirs=[tsfile_shared_dir] if system != "Windows" else None,
        extra_compile_args=(
            ["-std=c++11"] if system != "Windows" else ["-std=c++11", "-DMS_WIN64"]
        ),
        language="c++",
    )
]


class BuildExt(build_ext):
    def build_extensions(self):
        numpy_include = np.get_include()
        for ext in self.extensions:
            ext.include_dirs.append(numpy_include)
        super().build_extensions()

    def finalize_options(self):
        if system == "Windows":
            self.compiler = "mingw32"
        super().finalize_options()


setup(
    name="tsfile",
    version=version,
    description="Tsfile reader and writer for python",
    url="https://tsfile.apache.org",
    author='"Apache TsFile"',
    packages=["tsfile"],
    license="Apache 2.0",
    ext_modules=cythonize(ext_modules_tsfile),
    cmdclass={"build_ext": BuildExt},
    include_dirs=[np.get_include()],
    package_dir={"tsfile": "./tsfile"},
    package_data={
        "tsfile": [
            "libtsfile.*",
            "*.pxd"
        ]
    },
    include_package_data=True,
)
