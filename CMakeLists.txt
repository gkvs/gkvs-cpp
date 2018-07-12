cmake_minimum_required(VERSION 3.5)

project(gkvs C CXX)

set(CMAKE_CXX_STANDARD 11)

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
  add_definitions(-D_WIN32_WINNT=0x600)
endif()

message(STATUS "Running on ${CMAKE_SYSTEM_NAME}")

message(STATUS "Direct build")

include(FindPkgConfig)

# LuaJIT

find_library(_LUAJIT_LIB luajit)
message(STATUS "Using luajit  ${_LUAJIT_LIB}")
list(APPEND LIB_LIST ${_LUAJIT_LIB})

# Msgpack

find_package(msgpack CONFIG REQUIRED)
message(STATUS "Using msgpack ${msgpack_VERSION}")

# RocksDB

find_library(_ROCKSDB_LIB librocksdb.a rocksdb)
message(STATUS "Using rocksdb ${_ROCKSDB_LIB}")

# Snappy

find_library(_SNAPPY_LIB snappy)
message(STATUS "Using snappy ${_SNAPPY_LIB}")
list(APPEND LIB_LIST ${_SNAPPY_LIB})

# Z-standard

find_library(_ZSTD_LIB zstd)
message(STATUS "Using zstd ${_ZSTD_LIB}")
list(APPEND LIB_LIST ${_ZSTD_LIB})

# LZ4

find_library(_LZ4_LIB lz4)
message(STATUS "Using lz4 ${_LZ4_LIB}")
list(APPEND LIB_LIST ${_LZ4_LIB})

# BZ2

find_library(_BZ2_LIB bz2)
message(STATUS "Using bz2 ${_BZ2_LIB}")
list(APPEND LIB_LIST ${_BZ2_LIB})

# REDIS

find_library(_REDIS_LIB libhiredis.a hiredis)
message(STATUS "Using hiredis ${_REDIS_LIB}")

# JSON

include_directories("modules/nlohmann_json/include")

# Aerospike

include_directories("modules/aerospike-client-c/modules/common/src/include")
include_directories("modules/aerospike-client-c/src/include")

set(_AS_LIB_PATH "modules/aerospike-client-c/target/${CMAKE_SYSTEM_NAME}-x86_64/lib")

file(GLOB _AS_LIB "${_AS_LIB_PATH}/libaerospike.a")
message(STATUS "Using Aerospike Client ${_AS_LIB}")

find_library(_Z_LIB z)
message(STATUS "Using z ${_Z_LIB}")
list(APPEND LIB_LIST ${_Z_LIB})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DAS_USE_LIBEVENT")

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

pkg_check_modules (protobuf protobuf>=3.5.1)

message(STATUS "Using protobuf ${protobuf_VERSION}")

set(_PROTOBUF_LIB ${protobuf_LIBRARIES})

find_program(_PROTOBUF_PROTOC
          NAMES protoc
          DOC "protoc compiler"
          )

# GRPC++

pkg_check_modules (gRPC++ gRPC++>=1.2.0)

message(STATUS "Using gRPC++ ${gRPC++_VERSION}")

set(_GRPC_GRPCPP grpc++)

find_program(_GRPC_CPP_PLUGIN_EXECUTABLE
        NAMES grpc_cpp_plugin
        DOC "grpc_cpp_plugin compiler"
        )



# GFLAGS

find_package(gflags CONFIG REQUIRED)
message("-- Using gflags ${gflags_VERSION}")
list(APPEND LIB_LIST gflags)

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

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/gen")

# Generated sources
set(gkvs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/gen/gkvs.pb.cc")
set(gkvs_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gen/gkvs.pb.h")
set(gkvs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/gen/gkvs.grpc.pb.cc")
set(gkvs_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gen/gkvs.grpc.pb.h")

add_custom_command(
        OUTPUT "${gkvs_proto_srcs}" "${gkvs_proto_hdrs}" "${gkvs_grpc_srcs}" "${gkvs_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}/gen"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}/gen"
        -I "${gkvs_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${gkvs_proto}"
        DEPENDS "${gkvs_proto}")

# Include generated *.pb.h files
include_directories("cmake-build-debug/gen")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/gen")
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
        "src/rocks_driver.cc"
        "src/crypto.cc"
        "src/script.cc"
        ${gkvs_proto_srcs}
        ${gkvs_grpc_srcs})

message("-- LIB_LIST ${LIB_LIST}")


target_link_libraries(gkvs
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIB}
        glog
        msgpackc
        ${_ROCKSDB_LIB}
        ${_REDIS_LIB}
        ${_AS_LIB}
        ${LIB_LIST})

