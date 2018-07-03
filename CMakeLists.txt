cmake_minimum_required(VERSION 3.5)

project(gkvs C CXX)

set(CMAKE_CXX_STANDARD 11)

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
  add_definitions(-D_WIN32_WINNT=0x600)
endif()

message(STATUS "Running on ${CMAKE_SYSTEM_NAME}")

include(FindPkgConfig)

# Msgpack

find_package(msgpack CONFIG REQUIRED)
message(STATUS "Using msgpack ${msgpack_VERSION}")

# RocksDB

if (_GKVS_SUPERBUILD)

  if(NOT rocksdb_DIR)
    set(rocksdb_DIR "${CMAKE_CURRENT_BINARY_DIR}/../rocksdb/lib/cmake/rocksdb")
  endif()

  message(STATUS "Using rocksdb_DIR ${rocksdb_DIR}")
  find_package(rocksdb CONFIG REQUIRED)
  set(_ROCKSDB_LIB rocksdb)
  message(STATUS "Using rocksdb ${rocksdb_VERSION}")

else()

  find_library(_ROCKSDB_LIB librocksdb.a rocksdb)
  message(STATUS "Using rocksdb ${_ROCKSDB_LIB}")

endif()

# REDIS

if (_GKVS_SUPERBUILD)

  include_directories("modules/hiredis")
  file(GLOB _REDIS_LIB "modules/hiredis/libhiredis.a")
  message(STATUS "Using Redis Client ${_REDIS_LIB}")

else()

  find_library(_REDIS_LIB libhiredis.a hiredis)
  message(STATUS "Using hiredis ${_REDIS_LIB}")

endif()


# JSON

include_directories("modules/nlohmann_json/include")

# Aerospike

include_directories("modules/aerospike-client-c/modules/common/src/include")

#if (_GKVS_SUPERBUILD)

  include_directories("modules/aerospike-client-c/src/include")

  set(_AS_LIB_PATH "modules/aerospike-client-c/target/${CMAKE_SYSTEM_NAME}-x86_64/lib")

  file(GLOB _AS_LIB "${_AS_LIB_PATH}/libaerospike.a")
  message(STATUS "Using Aerospike Client ${_AS_LIB}")


#else()

#  find_library(_AS_LIB libaerospike.a aerospike)
#  message(STATUS "Using aerospike ${_AS_LIB}")

find_library(_Z_LIB z)
message(STATUS "Using z ${_Z_LIB}")
list(APPEND LIB_LIST ${_Z_LIB})

#  find_library(_M_LIB m)
#  message(STATUS "Using m ${_M_LIB}")
#  list(APPEND LIB_LIST m)

#  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DAS_USE_LIBEVENT")

#endif()


find_library(_EVENT_LIB libevent.a event)
message(STATUS "Using libevent ${_EVENT_LIB}")
list(APPEND LIB_LIST ${_EVENT_LIB})

find_library(_EVENT_LIB_PTHREADS libevent_pthreads.a event_pthreads)
message(STATUS "Using libevent_pthreads ${_EVENT_LIB_PTHREADS}")
list(APPEND LIB_LIST ${_EVENT_LIB_PTHREADS})

# GLOG

add_subdirectory(modules/glog ${CMAKE_CURRENT_BINARY_DIR}/glog EXCLUDE_FROM_ALL)
message(STATUS "Using GLOG via add_subdirectory modules/glog")

# Protobuf

if (_GKVS_SUPERBUILD)

  set(protobuf_MODULE_COMPATIBLE TRUE)
  find_package(protobuf CONFIG REQUIRED)
  message(STATUS "Using protobuf ${protobuf_VERSION}")

  set(_PROTOBUF_LIB protobuf::libprotobuf)
  set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)

else()

  pkg_check_modules (protobuf protobuf>=3.5.1)

  message(STATUS "Using protobuf ${protobuf_VERSION}")

  set(_PROTOBUF_LIB ${protobuf_LIBRARIES})

  find_program(_PROTOBUF_PROTOC
          NAMES protoc
          DOC "protoc compiler"
          )

endif()

# GRPC++

if (_GKVS_SUPERBUILD)

  find_package(gRPC CONFIG REQUIRED)
  message(STATUS "Using gRPC ${gRPC_VERSION}")

  set(_GRPC_GRPCPP gRPC::grpc++)
  set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

else()

  pkg_check_modules (gRPC++ gRPC++>=1.2.0)

  message(STATUS "Using gRPC++ ${gRPC++_VERSION}")

  set(_GRPC_GRPCPP grpc++)

  find_program(_GRPC_CPP_PLUGIN_EXECUTABLE
          NAMES grpc_cpp_plugin
          DOC "grpc_cpp_plugin compiler"
          )

endif()

find_package(gflags CONFIG REQUIRED)
message("-- Using gflags ${gflags_VERSION}")

if (NOT _GKVS_SUPERBUILD)
  list(APPEND LIB_LIST gflags)
endif()

# OpenSSL

if(NOT OPENSSL_ROOT_DIR)
  set(OPENSSL_ROOT_DIR "$ENV{OPENSSL_ROOT_DIR}")
endif()

if(NOT OPENSSL_ROOT_DIR)
  set(OPENSSL_ROOT_DIR "/usr/local/Cellar/openssl/1.0.2o_2")
endif()

message("OpenSSL root dir: ${OPENSSL_ROOT_DIR}")

find_package(OpenSSL REQUIRED)

message("OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")
message("OpenSSL libraries: ${OPENSSL_LIBRARIES}")

include_directories(${OPENSSL_INCLUDE_DIR})
list(APPEND LIB_LIST ${OPENSSL_LIBRARIES})


get_filename_component(gkvs_proto "protos/gkvs.proto" ABSOLUTE)
get_filename_component(gkvs_proto_path "${gkvs_proto}" PATH)

# Generated sources
set(gkvs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/gkvs.pb.cc")
set(gkvs_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gkvs.pb.h")
set(gkvs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/gkvs.grpc.pb.cc")
set(gkvs_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gkvs.grpc.pb.h")

add_custom_command(
        OUTPUT "${gkvs_proto_srcs}" "${gkvs_proto_hdrs}" "${gkvs_grpc_srcs}" "${gkvs_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${gkvs_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${gkvs_proto}"
        DEPENDS "${gkvs_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories("src")
# Build client

add_executable(gkvs-cli
        "src/gkvs_client.cc"
        ${gkvs_proto_srcs}
        ${gkvs_grpc_srcs})

target_link_libraries(gkvs-cli
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIB}
        glog
        gflags)

# Build server

add_executable(gkvs
        "src/gkvs_server.cc"
        "src/helper.cc"
        "src/as_driver.cc"
        "src/as_driver_tests.cc"
        "src/as_helper.cc"
        "src/redis_driver.cc"
        "src/crypto.cc"
        ${gkvs_proto_srcs}
        ${gkvs_grpc_srcs})

target_link_libraries(gkvs
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIB}
        glog
        msgpackc
        ${_ROCKSDB_LIB}
        ${_REDIS_LIB}
        ${_AS_LIB}
        ${LIB_LIST})

