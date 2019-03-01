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

#ifndef PLASMA_COMMON_H
#define PLASMA_COMMON_H

#include <stddef.h>

#include <cstring>
#include <memory>
#include <string>
// TODO(pcm): Convert getopt and sscanf in the store to use more idiomatic C++
// and get rid of the next three lines:
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <unordered_map>

#include "plasma/compat.h"

#include "arrow/status.h"
#include "arrow/util/logging.h"
#ifdef PLASMA_GPU
#include "arrow/gpu/cuda_api.h"
#endif

namespace plasma {

enum class ObjectLocation : int32_t { Local, Remote, Nonexistent };

constexpr int64_t kUniqueIDSize = 20;

class ARROW_EXPORT UniqueID {
 public:
  static UniqueID from_binary(const std::string& binary);
  bool operator==(const UniqueID& rhs) const;
  const uint8_t* data() const;
  uint8_t* mutable_data();
  std::string binary() const;
  std::string hex() const;
  size_t hash() const;
  static int64_t size() { return kUniqueIDSize; }

 private:
  uint8_t id_[kUniqueIDSize];
};

static_assert(std::is_pod<UniqueID>::value, "UniqueID must be plain old data");

typedef UniqueID ObjectID;

/// Size of object hash digests.
constexpr int64_t kDigestSize = sizeof(uint64_t);

enum class ObjectRequestType : int {
  /// Query for object in the local plasma store.
  PLASMA_QUERY_LOCAL = 1,
  /// Query for object in the local plasma store or in a remote plasma store.
  PLASMA_QUERY_ANYWHERE
};

/// Object request data structure. Used for Wait.
struct ObjectRequest {
  /// The ID of the requested object. If ID_NIL request any object.
  ObjectID object_id;
  /// Request associated to the object. It can take one of the following values:
  ///  - PLASMA_QUERY_LOCAL: return if or when the object is available in the
  ///    local Plasma Store.
  ///  - PLASMA_QUERY_ANYWHERE: return if or when the object is available in
  ///    the system (i.e., either in the local or a remote Plasma Store).
  ObjectRequestType type;
  /// Object location. This can be
  ///  - ObjectLocation::Local: object is ready at the local Plasma Store.
  ///  - ObjectLocation::Remote: object is ready at a remote Plasma Store.
  ///  - ObjectLocation::Nonexistent: object does not exist in the system.
  ObjectLocation location;
};

enum class ObjectState : int {
  /// Object was created but not sealed in the local Plasma Store.
  PLASMA_CREATED = 1,
  /// Object is sealed and stored in the local Plasma Store.
  PLASMA_SEALED
};

/// This type is used by the Plasma store. It is here because it is exposed to
/// the eviction policy.
struct ObjectTableEntry {
  ObjectTableEntry();

  ~ObjectTableEntry();

  /// Memory mapped file containing the object.
  int fd;
  /// Device number.
  int device_num;
  /// Size of the underlying map.
  int64_t map_size;
  /// Offset from the base of the mmap.
  ptrdiff_t offset;
  /// Pointer to the object data. Needed to free the object.
  uint8_t* pointer;
  /// Size of the object in bytes.
  int64_t data_size;
  /// Size of the object metadata in bytes.
  int64_t metadata_size;
#ifdef PLASMA_GPU
  /// IPC GPU handle to share with clients.
  std::shared_ptr<::arrow::gpu::CudaIpcMemHandle> ipc_handle;
#endif
  /// Number of clients currently using this object.
  int ref_count;
  /// Unix epoch of when this object was created.
  int64_t create_time;
  /// How long creation of this object took.
  int64_t construct_duration;

  /// The state of the object, e.g., whether it is open or sealed.
  ObjectState state;
  /// The digest of the object. Used to see if two objects are the same.
  unsigned char digest[kDigestSize];
};

/// Mapping from ObjectIDs to information about the object.
typedef std::unordered_map<ObjectID, std::unique_ptr<ObjectTableEntry>> ObjectTable;

/// Globally accessible reference to plasma store configuration.
/// TODO(pcm): This can be avoided with some refactoring of existing code
/// by making it possible to pass a context object through dlmalloc.
struct PlasmaStoreInfo;
extern const PlasmaStoreInfo* plasma_config;
}  // namespace plasma

namespace std {
template <>
struct hash<::plasma::UniqueID> {
  size_t operator()(const ::plasma::UniqueID& id) const { return id.hash(); }
};
}  // namespace std

#endif  // PLASMA_COMMON_H
