# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True


cdef class MemoryPool:

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly"
                        .format(self.__class__.__name__))

    cdef void init(self, CMemoryPool* pool):
        self.pool = pool

    def bytes_allocated(self):
        return self.pool.bytes_allocated()


cdef CMemoryPool* maybe_unbox_memory_pool(MemoryPool memory_pool):
    if memory_pool is None:
        return c_get_memory_pool()
    else:
        return memory_pool.pool


cdef class LoggingMemoryPool(MemoryPool):
    cdef:
        unique_ptr[CLoggingMemoryPool] logging_pool

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly"
                        .format(self.__class__.__name__))

    def __cinit__(self, MemoryPool pool):
        self.logging_pool.reset(new CLoggingMemoryPool(pool.pool))
        self.init(self.logging_pool.get())


cdef class ProxyMemoryPool(MemoryPool):
    """
    Derived MemoryPool class that tracks the number of bytes and
    maximum memory allocated through its direct calls.
    """
    cdef:
        unique_ptr[CProxyMemoryPool] proxy_pool

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly. "
                        "Use pyarrow.proxy_memory_pool instead."
                        .format(self.__class__.__name__))


def default_memory_pool():
    cdef:
        MemoryPool pool = MemoryPool.__new__(MemoryPool)
    pool.init(c_get_memory_pool())
    return pool


def proxy_memory_pool(MemoryPool parent):
    """
    Derived MemoryPool class that tracks the number of bytes and
    maximum memory allocated through its direct calls.
    """
    cdef ProxyMemoryPool out = ProxyMemoryPool.__new__(ProxyMemoryPool)
    out.proxy_pool.reset(new CProxyMemoryPool(parent.pool))
    out.init(out.proxy_pool.get())
    return out


def set_memory_pool(MemoryPool pool):
    c_set_default_memory_pool(pool.pool)


cdef MemoryPool _default_memory_pool = default_memory_pool()
cdef LoggingMemoryPool _logging_memory_pool = LoggingMemoryPool.__new__(
    LoggingMemoryPool, _default_memory_pool)


def log_memory_allocations(enable=True):
    """
    Enable or disable memory allocator logging for debugging purposes

    Parameters
    ----------
    enable : boolean, default True
        Pass False to disable logging
    """
    if enable:
        set_memory_pool(_logging_memory_pool)
    else:
        set_memory_pool(_default_memory_pool)


def total_allocated_bytes():
    cdef CMemoryPool* pool = c_get_memory_pool()
    return pool.bytes_allocated()
