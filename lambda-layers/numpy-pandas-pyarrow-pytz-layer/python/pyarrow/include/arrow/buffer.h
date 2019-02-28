// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_BUFFER_H
#define ARROW_BUFFER_H

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

// ----------------------------------------------------------------------
// Buffer classes

/// \class Buffer
/// \brief Object containing a pointer to a piece of contiguous memory with a
/// particular size. Base class does not own its memory
///
/// Buffers have two related notions of length: size and capacity. Size is
/// the number of bytes that might have valid data. Capacity is the number
/// of bytes that where allocated for the buffer in total.
///
/// The following invariant is always true: Size < Capacity
class ARROW_EXPORT Buffer {
 public:
  /// \brief Construct from buffer and size without copying memory
  ///
  /// \param[in] data a memory buffer
  /// \param[in] size buffer size
  ///
  /// \note The passed memory must be kept alive through some other means
  Buffer(const uint8_t* data, int64_t size)
      : is_mutable_(false),
        data_(data),
        mutable_data_(NULLPTR),
        size_(size),
        capacity_(size) {}

  /// \brief Construct from std::string without copying memory
  ///
  /// \param[in] data a std::string object
  ///
  /// \note The std::string must stay alive for the lifetime of the Buffer, so
  /// temporary rvalue strings must be stored in an lvalue somewhere
  explicit Buffer(const std::string& data)
      : Buffer(reinterpret_cast<const uint8_t*>(data.c_str()),
               static_cast<int64_t>(data.size())) {}

  virtual ~Buffer() = default;

  /// An offset into data that is owned by another buffer, but we want to be
  /// able to retain a valid pointer to it even after other shared_ptr's to the
  /// parent buffer have been destroyed
  ///
  /// This method makes no assertions about alignment or padding of the buffer but
  /// in general we expected buffers to be aligned and padded to 64 bytes.  In the future
  /// we might add utility methods to help determine if a buffer satisfies this contract.
  Buffer(const std::shared_ptr<Buffer>& parent, const int64_t offset, const int64_t size)
      : Buffer(parent->data() + offset, size) {
    parent_ = parent;
  }

  bool is_mutable() const { return is_mutable_; }

  /// Return true if both buffers are the same size and contain the same bytes
  /// up to the number of compared bytes
  bool Equals(const Buffer& other, int64_t nbytes) const;

  /// Return true if both buffers are the same size and contain the same bytes
  bool Equals(const Buffer& other) const;

  /// Copy a section of the buffer into a new Buffer.
  Status Copy(const int64_t start, const int64_t nbytes, MemoryPool* pool,
              std::shared_ptr<Buffer>* out) const;

  /// Copy a section of the buffer using the default memory pool into a new Buffer.
  Status Copy(const int64_t start, const int64_t nbytes,
              std::shared_ptr<Buffer>* out) const;

  /// Zero bytes in padding, i.e. bytes between size_ and capacity_.
  void ZeroPadding() {
#ifndef NDEBUG
    CheckMutable();
#endif
    memset(mutable_data_ + size_, 0, static_cast<size_t>(capacity_ - size_));
  }

  /// \brief Construct a new buffer that owns its memory from a std::string
  ///
  /// \param[in] data a std::string object
  /// \param[in] pool a memory pool
  /// \param[out] out the created buffer
  ///
  /// \return Status message
  static Status FromString(const std::string& data, MemoryPool* pool,
                           std::shared_ptr<Buffer>* out);

  /// \brief Construct a new buffer that owns its memory from a std::string
  /// using the default memory pool
  static Status FromString(const std::string& data, std::shared_ptr<Buffer>* out);

  /// \brief Construct an immutable buffer that takes ownership of the contents
  /// of an std::string
  /// \param[in] data an rvalue-reference of a string
  /// \return a new Buffer instance
  static std::shared_ptr<Buffer> FromString(std::string&& data);

  /// \brief Create buffer referencing typed memory with some length without
  /// copying
  /// \param[in] data the typed memory as C array
  /// \param[in] length the number of values in the array
  /// \return a new shared_ptr<Buffer>
  template <typename T, typename SizeType = int64_t>
  static std::shared_ptr<Buffer> Wrap(const T* data, SizeType length) {
    return std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(data),
                                    static_cast<int64_t>(sizeof(T) * length));
  }

  /// \brief Create buffer referencing std::vector with some length without
  /// copying
  /// \param[in] data the vector to be referenced. If this vector is changed,
  /// the buffer may become invalid
  /// \return a new shared_ptr<Buffer>
  template <typename T>
  static std::shared_ptr<Buffer> Wrap(const std::vector<T>& data) {
    return std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(data.data()),
                                    static_cast<int64_t>(sizeof(T) * data.size()));
  }

  /// \brief Copy buffer contents into a new std::string
  /// \return std::string
  /// \note Can throw std::bad_alloc if buffer is large
  std::string ToString() const;

  int64_t capacity() const { return capacity_; }
  const uint8_t* data() const { return data_; }

  uint8_t* mutable_data() {
#ifndef NDEBUG
    CheckMutable();
#endif
    return mutable_data_;
  }

  int64_t size() const { return size_; }

  std::shared_ptr<Buffer> parent() const { return parent_; }

 protected:
  bool is_mutable_;
  const uint8_t* data_;
  uint8_t* mutable_data_;
  int64_t size_;
  int64_t capacity_;

  // null by default, but may be set
  std::shared_ptr<Buffer> parent_;

  void CheckMutable() const;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Buffer);
};

/// Construct a view on passed buffer at the indicated offset and length. This
/// function cannot fail and does not error checking (except in debug builds)
static inline std::shared_ptr<Buffer> SliceBuffer(const std::shared_ptr<Buffer>& buffer,
                                                  const int64_t offset,
                                                  const int64_t length) {
  return std::make_shared<Buffer>(buffer, offset, length);
}

static inline std::shared_ptr<Buffer> SliceBuffer(const std::shared_ptr<Buffer>& buffer,
                                                  const int64_t offset) {
  int64_t length = buffer->size() - offset;
  return SliceBuffer(buffer, offset, length);
}

/// Construct a mutable buffer slice. If the parent buffer is not mutable, this
/// will abort in debug builds
ARROW_EXPORT
std::shared_ptr<Buffer> SliceMutableBuffer(const std::shared_ptr<Buffer>& buffer,
                                           const int64_t offset, const int64_t length);

/// \class MutableBuffer
/// \brief A Buffer whose contents can be mutated. May or may not own its data.
class ARROW_EXPORT MutableBuffer : public Buffer {
 public:
  MutableBuffer(uint8_t* data, const int64_t size) : Buffer(data, size) {
    mutable_data_ = data;
    is_mutable_ = true;
  }

  MutableBuffer(const std::shared_ptr<Buffer>& parent, const int64_t offset,
                const int64_t size);

  /// \brief Create buffer referencing typed memory with some length
  /// \param[in] data the typed memory as C array
  /// \param[in] length the number of values in the array
  /// \return a new shared_ptr<Buffer>
  template <typename T, typename SizeType = int64_t>
  static std::shared_ptr<Buffer> Wrap(T* data, SizeType length) {
    return std::make_shared<MutableBuffer>(reinterpret_cast<uint8_t*>(data),
                                           static_cast<int64_t>(sizeof(T) * length));
  }

 protected:
  MutableBuffer() : Buffer(NULLPTR, 0) {}
};

/// \class ResizableBuffer
/// \brief A mutable buffer that can be resized
class ARROW_EXPORT ResizableBuffer : public MutableBuffer {
 public:
  /// Change buffer reported size to indicated size, allocating memory if
  /// necessary.  This will ensure that the capacity of the buffer is a multiple
  /// of 64 bytes as defined in Layout.md.
  /// Consider using ZeroPadding afterwards, in case you return buffer to a reader.
  ///
  /// @param shrink_to_fit On deactivating this option, the capacity of the Buffer won't
  /// decrease.
  virtual Status Resize(const int64_t new_size, bool shrink_to_fit = true) = 0;

  /// Ensure that buffer has enough memory allocated to fit the indicated
  /// capacity (and meets the 64 byte padding requirement in Layout.md).
  /// It does not change buffer's reported size and doesn't zero the padding.
  virtual Status Reserve(const int64_t new_capacity) = 0;

  template <class T>
  Status TypedResize(const int64_t new_nb_elements, bool shrink_to_fit = true) {
    return Resize(sizeof(T) * new_nb_elements, shrink_to_fit);
  }

  template <class T>
  Status TypedReserve(const int64_t new_nb_elements) {
    return Reserve(sizeof(T) * new_nb_elements);
  }

 protected:
  ResizableBuffer(uint8_t* data, int64_t size) : MutableBuffer(data, size) {}
};

/// \brief Allocate a fixed size mutable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(MemoryPool* pool, const int64_t size, std::shared_ptr<Buffer>* out);

/// \brief Allocate a fixed size mutable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(MemoryPool* pool, const int64_t size, std::unique_ptr<Buffer>* out);

/// \brief Allocate a fixed-size mutable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(const int64_t size, std::shared_ptr<Buffer>* out);

/// \brief Allocate a fixed-size mutable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(const int64_t size, std::unique_ptr<Buffer>* out);

/// \brief Allocate a resizeable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(MemoryPool* pool, const int64_t size,
                               std::shared_ptr<ResizableBuffer>* out);

/// \brief Allocate a resizeable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(MemoryPool* pool, const int64_t size,
                               std::unique_ptr<ResizableBuffer>* out);

/// \brief Allocate a resizeable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(const int64_t size, std::shared_ptr<ResizableBuffer>* out);

/// \brief Allocate a resizeable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(const int64_t size, std::unique_ptr<ResizableBuffer>* out);

/// \brief Allocate a zero-initialized bitmap buffer from a memory pool
///
/// \param[in] pool memory pool to allocate memory from
/// \param[in] length size in bits of bitmap to allocate
/// \param[out] out the resulting buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateEmptyBitmap(MemoryPool* pool, int64_t length,
                           std::shared_ptr<Buffer>* out);

/// \brief Allocate a zero-initialized bitmap buffer from the default memory pool
///
/// \param[in] length size in bits of bitmap to allocate
/// \param[out] out the resulting buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateEmptyBitmap(int64_t length, std::shared_ptr<Buffer>* out);

// ----------------------------------------------------------------------
// Buffer builder classes

/// \class BufferBuilder
/// \brief A class for incrementally building a contiguous chunk of in-memory data
class ARROW_EXPORT BufferBuilder {
 public:
  explicit BufferBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : pool_(pool), data_(NULLPTR), capacity_(0), size_(0) {}

  /// \brief Resizes the buffer to the nearest multiple of 64 bytes
  ///
  /// \param elements the new capacity of the of the builder. Will be rounded
  /// up to a multiple of 64 bytes for padding
  /// \param shrink_to_fit if new capacity smaller than existing size,
  /// reallocate internal buffer. Set to false to avoid reallocations when
  /// shrinking the builder
  /// \return Status
  Status Resize(const int64_t elements, bool shrink_to_fit = true) {
    // Resize(0) is a no-op
    if (elements == 0) {
      return Status::OK();
    }
    int64_t old_capacity = capacity_;

    if (buffer_ == NULLPTR) {
      ARROW_RETURN_NOT_OK(AllocateResizableBuffer(pool_, elements, &buffer_));
    } else {
      ARROW_RETURN_NOT_OK(buffer_->Resize(elements, shrink_to_fit));
    }
    capacity_ = buffer_->capacity();
    data_ = buffer_->mutable_data();
    if (capacity_ > old_capacity) {
      memset(data_ + old_capacity, 0, capacity_ - old_capacity);
    }
    return Status::OK();
  }

  /// \brief Ensure that builder can accommodate the additional number of bytes
  /// without the need to perform allocations
  ///
  /// \param size number of additional bytes to make space for
  /// \return Status
  Status Reserve(const int64_t size) { return Resize(size_ + size, false); }

  Status Append(const void* data, int64_t length) {
    if (capacity_ < length + size_) {
      int64_t new_capacity = BitUtil::NextPower2(length + size_);
      ARROW_RETURN_NOT_OK(Resize(new_capacity));
    }
    UnsafeAppend(data, length);
    return Status::OK();
  }

  template <size_t NBYTES>
  Status Append(const std::array<uint8_t, NBYTES>& data) {
    constexpr auto nbytes = static_cast<int64_t>(NBYTES);
    if (capacity_ < nbytes + size_) {
      int64_t new_capacity = BitUtil::NextPower2(nbytes + size_);
      ARROW_RETURN_NOT_OK(Resize(new_capacity));
    }

    std::copy(data.cbegin(), data.cend(), data_ + size_);
    size_ += nbytes;
    return Status::OK();
  }

  // Advance pointer and zero out memory
  Status Advance(const int64_t length) {
    if (capacity_ < length + size_) {
      int64_t new_capacity = BitUtil::NextPower2(length + size_);
      ARROW_RETURN_NOT_OK(Resize(new_capacity));
    }
    memset(data_ + size_, 0, static_cast<size_t>(length));
    size_ += length;
    return Status::OK();
  }

  // Unsafe methods don't check existing size
  void UnsafeAppend(const void* data, int64_t length) {
    memcpy(data_ + size_, data, static_cast<size_t>(length));
    size_ += length;
  }

  Status Finish(std::shared_ptr<Buffer>* out, bool shrink_to_fit = true) {
    ARROW_RETURN_NOT_OK(Resize(size_, shrink_to_fit));
    *out = buffer_;
    Reset();
    return Status::OK();
  }

  void Reset() {
    buffer_ = NULLPTR;
    capacity_ = size_ = 0;
  }

  int64_t capacity() const { return capacity_; }
  int64_t length() const { return size_; }
  const uint8_t* data() const { return data_; }

 protected:
  std::shared_ptr<ResizableBuffer> buffer_;
  MemoryPool* pool_;
  uint8_t* data_;
  int64_t capacity_;
  int64_t size_;
};

template <typename T>
class ARROW_EXPORT TypedBufferBuilder : public BufferBuilder {
 public:
  explicit TypedBufferBuilder(MemoryPool* pool) : BufferBuilder(pool) {}

  Status Append(T arithmetic_value) {
    static_assert(std::is_arithmetic<T>::value,
                  "Convenience buffer append only supports arithmetic types");
    return BufferBuilder::Append(reinterpret_cast<uint8_t*>(&arithmetic_value),
                                 sizeof(T));
  }

  Status Append(const T* arithmetic_values, int64_t num_elements) {
    static_assert(std::is_arithmetic<T>::value,
                  "Convenience buffer append only supports arithmetic types");
    return BufferBuilder::Append(reinterpret_cast<const uint8_t*>(arithmetic_values),
                                 num_elements * sizeof(T));
  }

  void UnsafeAppend(T arithmetic_value) {
    static_assert(std::is_arithmetic<T>::value,
                  "Convenience buffer append only supports arithmetic types");
    BufferBuilder::UnsafeAppend(reinterpret_cast<uint8_t*>(&arithmetic_value), sizeof(T));
  }

  void UnsafeAppend(const T* arithmetic_values, int64_t num_elements) {
    static_assert(std::is_arithmetic<T>::value,
                  "Convenience buffer append only supports arithmetic types");
    BufferBuilder::UnsafeAppend(reinterpret_cast<const uint8_t*>(arithmetic_values),
                                num_elements * sizeof(T));
  }

  const T* data() const { return reinterpret_cast<const T*>(data_); }
  int64_t length() const { return size_ / sizeof(T); }
  int64_t capacity() const { return capacity_ / sizeof(T); }
};

}  // namespace arrow

#endif  // ARROW_BUFFER_H
