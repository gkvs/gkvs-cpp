cmake_minimum_required(VERSION 3.5)

project(gkvs C CXX)

set(CMAKE_CXX_STANDARD 11)

message(STATUS "Running on ${CMAKE_SYSTEM_NAME}")

# Msgpack

find_package(msgpack CONFIG REQUIRED)
message(STATUS "Using msgpack ${msgpack_VERSION}")

# RocksDB

if(NOT rocksdb_DIR)
    set(rocksdb_DIR "${CMAKE_CURRENT_BINARY_DIR}/../rocksdb/lib/cmake/rocksdb")
endif()

message(STATUS "Using rocksdb_DIR ${rocksdb_DIR}")
find_package(rocksdb CONFIG REQUIRED)
message(STATUS "Using rocksdb ${rocksdb_VERSION}")

# REDIS

include_directories("../modules/hiredis")
file(GLOB _REDIS_LIB "../modules/hiredis/libhiredis.a")
message(STATUS "Using Redis Client ${_REDIS_LIB}")

# JSON

include_directories("../modules/nlohmann_json/include")

# Aerospike

include_directories("../modules/aerospike-client-c/src/include")
include_directories("../modules/aerospike-client-c/modules/common/src/include")

set(_AS_LIB_PATH "../modules/aerospike-client-c/target/${CMAKE_SYSTEM_NAME}-x86_64/lib")

file(GLOB _AS_LIB "${_AS_LIB_PATH}/libaerospike.a")
message(STATUS "Using Aerospike Client ${_AS_LIB}")

find_library(_EVENT libevent.a event)
message(STATUS "Using libevent ${_EVENT}")

find_library(_EVENT_PTHREADS libevent_pthreads.a event_pthreads)
message(STATUS "Using libevent_pthreads ${_EVENT_PTHREADS}")


# GRPC and protobuf

add_subdirectory(../modules/glog ${CMAKE_CURRENT_BINARY_DIR}/glog EXCLUDE_FROM_ALL)
message(STATUS "Using GLOG via add_subdirectory.")

# After using add_subdirectory, we can now use the grpc targets directly from
# this build.

set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

find_package(gflags CONFIG REQUIRED)

# OpenSSL

find_package(OpenSSL REQUIRED)

message("OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")
message("OpenSSL libraries: ${OPENSSL_LIBRARIES}")

include_directories(${OPENSSL_INCLUDE_DIR})
list(APPEND LIB_LIST ${OPENSSL_LIBRARIES})

get_filename_component(gkvs_proto "../protos/gkvs.proto" ABSOLUTE)
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

# Build client

add_executable(gkvs-cli
        "gkvs_client.cc"
        ${gkvs_proto_srcs}
        ${gkvs_grpc_srcs})

target_link_libraries(gkvs-cli
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        glog
        gflags)

# Build server

add_executable(gkvs
        "gkvs_server.cc"
        "helper.cc"
        "as_driver.cc"
        "as_driver_tests.cc"
        "as_helper.cc"
        "redis_driver.cc"
        "crypto.cc"
        ${gkvs_proto_srcs}
        ${gkvs_grpc_srcs})

target_link_libraries(gkvs
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        glog
        msgpackc
        rocksdb
        ${_REDIS_LIB}
        ${_AS_LIB}
        ${LIB_LIST}
        ${_EVENT}
        ${_EVENT_PTHREADS})

