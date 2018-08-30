cmake_minimum_required(VERSION 3.5)

project(gkvs C CXX)

set(CMAKE_CXX_STANDARD 11)

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
else()
  add_definitions(-D_WIN32_WINNT=0x600)
endif()

#if (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
#  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed -v")
#endif()

message(STATUS "Running on ${CMAKE_SYSTEM_NAME}")
message(STATUS "Direct build")

include(FindPkgConfig)

# JSON

include_directories("modules/nlohmann_json/include")

# REDIS

find_library(_REDIS_LIB libhiredis.a hiredis HINTS "modules/hiredis")
message(STATUS "Using hiredis ${_REDIS_LIB}")
list(APPEND SERVER_LIB_LIST ${_REDIS_LIB})

# AEROSPIKE

include_directories("modules/aerospike-client-c/modules/common/src/include")
include_directories("modules/aerospike-client-c/src/include")

find_library(_AEROSPIKE_LIB NAMES libaerospike.a aerospike HINTS "modules/aerospike-client-c/target/${CMAKE_SYSTEM_NAME}-x86_64/lib")
message(STATUS "Using Aerospike Client ${_AEROSPIKE_LIB}")
list(APPEND SERVER_LIB_LIST ${_AEROSPIKE_LIB})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DAS_USE_LIBEVENT")

# LuaJIT

find_library(_LUAJIT_LIB NAMES libluajit-5.1.a luajit-5.1)
message(STATUS "Using luajit  ${_LUAJIT_LIB}")
list(APPEND SERVER_LIB_LIST ${_LUAJIT_LIB})

# Msgpack

find_library(_MSGPACK_LIB libmsgpackc.a msgpackc HINTS "modules/msgpack-c/build")
message(STATUS "Using msgpack lib ${_MSGPACK_LIB}")
list(APPEND CLIENT_LIB_LIST ${_MSGPACK_LIB})
list(APPEND SERVER_LIB_LIST ${_MSGPACK_LIB})

# GRPC++

find_library(_GRPCPP_LIB NAMES libgrpc++.a grpc++ HINTS "modules/grpc/build")
message(STATUS "Using gRPC++ ${_GRPCPP_LIB}")
list(APPEND CLIENT_LIB_LIST ${_GRPCPP_LIB})
list(APPEND SERVER_LIB_LIST ${_GRPCPP_LIB})

# GRPC

find_library(_GRPC_LIB NAMES libgrpc.a grpc HINTS "modules/grpc/build")
message(STATUS "Using gRPC ${_GRPC_LIB}")
list(APPEND CLIENT_LIB_LIST ${_GRPC_LIB})
list(APPEND SERVER_LIB_LIST ${_GRPC_LIB})

if (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")

## Boring SSL

find_library(_BORING_SSL_LIB NAMES libssl.a ssl HINTS "modules/grpc/build/third_party/boringssl/ssl")
message(STATUS "Using Boring SSL ${_BORING_SSL_LIB}")
list(APPEND CLIENT_LIB_LIST ${_BORING_SSL_LIB})
list(APPEND SERVER_LIB_LIST ${_BORING_SSL_LIB})

# Boring Crypto

find_library(_BORING_CRYPTO_LIB NAMES libcrypto.a crypto HINTS "modules/grpc/build/third_party/boringssl/crypto")
message(STATUS "Using Boring Crypto ${_BORING_CRYPTO_LIB}")
list(APPEND CLIENT_LIB_LIST ${_BORING_CRYPTO_LIB})
list(APPEND SERVER_LIB_LIST ${_BORING_CRYPTO_LIB})

endif()

# Protobuf

find_library(_PROTOBUF_LIB NAMES libprotobuf.a protobuf HINTS "modules/grpc/build/third_party/protobuf")
message(STATUS "Using protobuf ${_PROTOBUF_LIB}")
list(APPEND CLIENT_LIB_LIST ${_PROTOBUF_LIB})
list(APPEND SERVER_LIB_LIST ${_PROTOBUF_LIB})

# GPR
find_library(_GPR_LIB NAMES libgpr.a gpr HINTS "modules/grpc/build")
message(STATUS "Using GPR ${_GPR_LIB}")
list(APPEND CLIENT_LIB_LIST ${_GPR_LIB})
list(APPEND SERVER_LIB_LIST ${_GPR_LIB})

# CARES

find_library(_CARES_LIB NAMES libcares.a cares HINTS "modules/grpc/build/third_party/cares/cares/lib")
message(STATUS "Using Cares ${_CARES_LIB}")
list(APPEND CLIENT_LIB_LIST ${_CARES_LIB})
list(APPEND SERVER_LIB_LIST ${_CARES_LIB})

# Address Sorting
find_library(_ADDRESS_SORTING_LIB NAMES libaddress_sorting.a address_sorting HINTS "modules/grpc/build")
message(STATUS "Using Address Sorting ${_ADDRESS_SORTING_LIB}")
list(APPEND CLIENT_LIB_LIST ${_ADDRESS_SORTING_LIB})
list(APPEND SERVER_LIB_LIST ${_ADDRESS_SORTING_LIB})

# GLOG

find_library(_GLOG_LIB NAMES libglog.a glog)
message(STATUS "Using glog ${_GLOG_LIB}")
list(APPEND CLIENT_LIB_LIST ${_GLOG_LIB})
list(APPEND SERVER_LIB_LIST ${_GLOG_LIB})

# GFLAGS

set(gflags_STATIC ON)
find_package(gflags CONFIG REQUIRED)
message("-- Using gflags ${gflags_VERSION} libs ${gflags_LIBRARIES}")
list(APPEND CLIENT_LIB_LIST ${gflags_LIBRARIES})
list(APPEND SERVER_LIB_LIST ${gflags_LIBRARIES})

# OpenSSL

if(NOT OPENSSL_ROOT_DIR)
  set(OPENSSL_ROOT_DIR "$ENV{OPENSSL_ROOT_DIR}")
endif()
message("OpenSSL Root Dir: ${OPENSSL_ROOT_DIR}")

#set(OPENSSL_STATIC ON)
#set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL REQUIRED)

message("OpenSSL Include Dir: ${OPENSSL_INCLUDE_DIR}")
message("OpenSSL Libraries: ${OPENSSL_LIBRARIES}")

include_directories(${OPENSSL_INCLUDE_DIR})
list(APPEND CLIENT_LIB_LIST ${OPENSSL_LIBRARIES})
list(APPEND SERVER_LIB_LIST ${OPENSSL_LIBRARIES})

# Z

find_library(_Z_LIB NAMES libz.a z)
message(STATUS "Using zlib ${_Z_LIB}")
list(APPEND CLIENT_LIB_LIST ${_Z_LIB})
list(APPEND SERVER_LIB_LIST ${_Z_LIB})

# Linux System Libs

if (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  list(APPEND SERVER_LIB_LIST "dl")
  list(APPEND SERVER_LIB_LIST "c")
  list(APPEND SERVER_LIB_LIST "m")
endif()

get_filename_component(gkvs_proto "protos/gkvs.proto" ABSOLUTE)
get_filename_component(gkvs_proto_path "${gkvs_proto}" PATH)

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/gen")

# LibEvent

find_library(_EVENT_LIB NAMES libevent.a event)
message(STATUS "Using libevent ${_EVENT_LIB}")
list(APPEND SERVER_LIB_LIST ${_EVENT_LIB})

# Event-PThreads

find_library(_EVENT_PTHREADS_LIB NAMES libevent_pthreads.a event_pthreads)
message(STATUS "Using libevent_pthreads ${_EVENT_PTHREADS_LIB}")
list(APPEND SERVER_LIB_LIST ${_EVENT_PTHREADS_LIB})

# PThreads

#find_library(_PTHREADS_LIB NAMES libpthread.a pthread)
#message(STATUS "Using pthreads ${_PTHREADS_LIB}")
#list(APPEND CLIENT_LIB_LIST ${_PTHREADS_LIB})
#list(APPEND SERVER_LIB_LIST ${_PTHREADS_LIB})

# Protoc

find_program(_PROTOC_EXEC
          NAMES protoc
          DOC "protoc compiler"
          )
message(STATUS "Using protoc ${_PROTOC_EXEC}")

# GRPC++ Plugin

find_program(_GRPCPP_PLUGIN_EXEC
        NAMES grpc_cpp_plugin
        DOC "grpc_cpp_plugin compiler"
        )
message(STATUS "Using gRPC++ Plugin ${_GRPCPP_PLUGIN_EXEC}")

# Generated sources
set(gkvs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/gen/gkvs.pb.cc")
set(gkvs_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gen/gkvs.pb.h")
set(gkvs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/gen/gkvs.grpc.pb.cc")
set(gkvs_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gen/gkvs.grpc.pb.h")

add_custom_command(
        OUTPUT "${gkvs_proto_srcs}" "${gkvs_proto_hdrs}" "${gkvs_grpc_srcs}" "${gkvs_grpc_hdrs}"
        COMMAND ${_PROTOC_EXEC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}/gen"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}/gen"
        -I "${gkvs_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPCPP_PLUGIN_EXEC}"
        "${gkvs_proto}"
        DEPENDS "${gkvs_proto}")

# Include generated *.pb.h files
include_directories("cmake-build-debug/gen")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/gen")
include_directories("src")

# Build Client

add_executable(gkvs-cli
        "src/gkvs_client.cc"
        ${gkvs_proto_srcs}
        ${gkvs_grpc_srcs})

message("-- CLIENT_LIB_LIST ${CLIENT_LIB_LIST}")

target_link_libraries(gkvs-cli 
        ${CLIENT_LIB_LIST})

# Build Server

add_executable(gkvs
        "src/gkvs_server.cc"
        "src/helper.cc"
        "src/as_driver.cc"
        "src/as_driver_tests.cc"
        "src/as_helper.cc"
        "src/redis_driver.cc"
        "src/crypto.cc"
        "src/script.cc"
        ${gkvs_proto_srcs}
        ${gkvs_grpc_srcs})

message("-- SERVER_LIB_LIST ${SERVER_LIB_LIST}")

target_link_libraries(gkvs
        ${SERVER_LIB_LIST})

