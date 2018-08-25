cmake_minimum_required(VERSION 3.5)

project(gkvs C CXX)

set(CMAKE_CXX_STANDARD 11)

if(NOT MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
    add_definitions(-D_WIN32_WINNT=0x600)
endif()

message(STATUS "Running on ${CMAKE_SYSTEM_NAME}")
message(STATUS "Superbuild")

# LuaJIT

find_library(_LUAJIT_LIB luajit)
message(STATUS "Using luajit  ${_LUAJIT_LIB}")
list(APPEND LIB_LIST ${_LUAJIT_LIB})

# Msgpack

find_package(msgpack CONFIG REQUIRED)
message(STATUS "Using msgpack ${msgpack_VERSION}")


# REDIS

include_directories("../modules/hiredis")
file(GLOB _REDIS_LIB "../modules/hiredis/libhiredis.a")
message(STATUS "Using Redis Client ${_REDIS_LIB}")


# JSON

include_directories("../modules/nlohmann_json/include")

# Aerospike

include_directories("../modules/aerospike-client-c/modules/common/src/include")
include_directories("../modules/aerospike-client-c/src/include")

set(_AS_LIB_PATH "../modules/aerospike-client-c/target/${CMAKE_SYSTEM_NAME}-x86_64/lib")

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

add_subdirectory(../modules/glog ${CMAKE_CURRENT_BINARY_DIR}/../glog EXCLUDE_FROM_ALL)
message(STATUS "Using GLOG via add_subdirectory modules/glog")

# Protobuf

if(NOT protobuf_DIR)
    set(protobuf_DIR "${CMAKE_CURRENT_BINARY_DIR}/../protobuf/lib/cmake/protobuf")
endif()

set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")

set(_PROTOBUF_LIB protobuf::libprotobuf)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)


# GRPC++

find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)


# GFLAGS

find_package(gflags CONFIG REQUIRED)
message("-- Using gflags ${gflags_VERSION}")

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

# Generate PROTOS

get_filename_component(gkvs_proto "../protos/gkvs.proto" ABSOLUTE)
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
include_directories("${CMAKE_CURRENT_BINARY_DIR}/gen")
# Build client

add_executable(gkvs-cli
        "gkvs_client.cc"
        ${gkvs_proto_srcs}
        ${gkvs_grpc_srcs})

target_link_libraries(gkvs-cli
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIB}
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
        "script.cc"
        ${gkvs_proto_srcs}
        ${gkvs_grpc_srcs})

target_link_libraries(gkvs
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIB}
        glog
        msgpackc
        ${_REDIS_LIB}
        ${_AS_LIB}
        ${LIB_LIST})

