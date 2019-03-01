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

# distutils: language = c++

from pyarrow.includes.libarrow cimport *

cdef extern from "arrow/gpu/cuda_api.h" namespace "arrow::gpu" nogil:

    cdef cppclass CCudaDeviceManager" arrow::gpu::CudaDeviceManager":
        @staticmethod
        CStatus GetInstance(CCudaDeviceManager** manager)
        CStatus GetContext(int gpu_number, shared_ptr[CCudaContext]* ctx)
        # CStatus CreateNewContext(int gpu_number,
        #                          shared_ptr[CCudaContext]* ctx)
        CStatus AllocateHost(int64_t nbytes,
                             shared_ptr[CCudaHostBuffer]* buffer)
        # CStatus FreeHost(void* data, int64_t nbytes)
        int num_devices() const

    cdef cppclass CCudaContext" arrow::gpu::CudaContext":
        shared_ptr[CCudaContext]  shared_from_this()
        # CStatus Close()
        CStatus Allocate(int64_t nbytes, shared_ptr[CCudaBuffer]* out)
        CStatus OpenIpcBuffer(const CCudaIpcMemHandle& ipc_handle,
                              shared_ptr[CCudaBuffer]* buffer)
        int64_t bytes_allocated() const

    cdef cppclass CCudaIpcMemHandle" arrow::gpu::CudaIpcMemHandle":
        @staticmethod
        CStatus FromBuffer(const void* opaque_handle,
                           shared_ptr[CCudaIpcMemHandle]* handle)
        CStatus Serialize(CMemoryPool* pool, shared_ptr[CBuffer]* out) const

    cdef cppclass CCudaBuffer" arrow::gpu::CudaBuffer"(CBuffer):
        CCudaBuffer(uint8_t* data, int64_t size,
                    const shared_ptr[CCudaContext]& context,
                    c_bool own_data=false, c_bool is_ipc=false)
        CCudaBuffer(const shared_ptr[CCudaBuffer]& parent,
                    const int64_t offset, const int64_t size)

        @staticmethod
        CStatus FromBuffer(shared_ptr[CBuffer] buffer,
                           shared_ptr[CCudaBuffer]* out)

        CStatus CopyToHost(const int64_t position, const int64_t nbytes,
                           void* out) const
        CStatus CopyFromHost(const int64_t position, const void* data,
                             int64_t nbytes)
        CStatus ExportForIpc(shared_ptr[CCudaIpcMemHandle]* handle)
        shared_ptr[CCudaContext] context() const

    cdef cppclass CCudaHostBuffer" arrow::gpu::CudaHostBuffer"(CMutableBuffer):
        pass

    cdef cppclass \
            CCudaBufferReader" arrow::gpu::CudaBufferReader"(CBufferReader):
        CCudaBufferReader(const shared_ptr[CBuffer]& buffer)
        CStatus Read(int64_t nbytes, int64_t* bytes_read, void* buffer)
        CStatus Read(int64_t nbytes, shared_ptr[CBuffer]* out)

    cdef cppclass \
            CCudaBufferWriter" arrow::gpu::CudaBufferWriter"(WritableFile):
        CCudaBufferWriter(const shared_ptr[CCudaBuffer]& buffer)
        CStatus Close()
        CStatus Flush()
        # CStatus Seek(int64_t position)
        CStatus Write(const void* data, int64_t nbytes)
        CStatus WriteAt(int64_t position, const void* data, int64_t nbytes)
        # CStatus Tell(int64_t* position) const
        CStatus SetBufferSize(const int64_t buffer_size)
        int64_t buffer_size()
        int64_t num_bytes_buffered() const

    CStatus AllocateCudaHostBuffer(const int64_t size,
                                   shared_ptr[CCudaHostBuffer]* out)

    # Cuda prefix is added to avoid picking up arrow::gpu functions
    # from arrow namespace.
    CStatus CudaSerializeRecordBatch" arrow::gpu::SerializeRecordBatch"\
        (const CRecordBatch& batch,
         CCudaContext* ctx,
         shared_ptr[CCudaBuffer]* out)
    CStatus CudaReadMessage" arrow::gpu::ReadMessage"\
        (CCudaBufferReader* reader,
         CMemoryPool* pool,
         unique_ptr[CMessage]* message)
    CStatus CudaReadRecordBatch" arrow::gpu::ReadRecordBatch"\
        (const shared_ptr[CSchema]& schema,
         const shared_ptr[CCudaBuffer]& buffer,
         CMemoryPool* pool, shared_ptr[CRecordBatch]* out)
