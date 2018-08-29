cmake_minimum_required(VERSION 3.5)

project(gkvs C CXX)

set(CMAKE_CXX_STANDARD 11)

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
else()
  add_definitions(-D_WIN32_WINNT=0x600)
endif()

if (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed -ldl -v")
endif()

message(STATUS "Running on ${CMAKE_SYSTEM_NAME}")
message(STATUS "Direct build")

include(FindPkgConfig)


# LuaJIT

find_library(_LUAJIT_LIB NAMES libluajit-5.1.a luajit-5.1)
message(STATUS "Using luajit  ${_LUAJIT_LIB}")
list(APPEND LIB_LIST ${_LUAJIT_LIB})

# Msgpack

find_library(_MSGPACK_LIB libmsgpackc.a msgpackc)
message(STATUS "Using msgpack lib ${_MSGPACK_LIB}")
list(APPEND LIB_LIST ${_MSGPACK_LIB})

# REDIS

find_library(_REDIS_LIB libhiredis.a hiredis)
message(STATUS "Using hiredis ${_REDIS_LIB}")
list(APPEND DRIVER_LIB_LIST ${_REDIS_LIB})

# JSON

include_directories("modules/nlohmann_json/include")

# Aerospike (Do not install it, becasue it compiles for different event loops)

include_directories("modules/aerospike-client-c/modules/common/src/include")
include_directories("modules/aerospike-client-c/src/include")

set(_AS_LIB_PATH "modules/aerospike-client-c/target/${CMAKE_SYSTEM_NAME}-x86_64/lib")

file(GLOB _AS_LIB "${_AS_LIB_PATH}/libaerospike.a")
message(STATUS "Using Aerospike Client ${_AS_LIB}")
list(APPEND DRIVER_LIB_LIST ${_AS_LIB})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DAS_USE_LIBEVENT")

# Z

find_library(_Z_LIB NAMES libz.a z)
message(STATUS "Using zlib ${_Z_LIB}")
list(APPEND LIB_LIST ${_Z_LIB})

# LibEvent

find_library(_EVENT_LIB NAMES libevent.a event)
message(STATUS "Using libevent ${_EVENT_LIB}")
list(APPEND DRIVER_LIB_LIST ${_EVENT_LIB})

# PThreads

find_library(_EVENT_LIB_PTHREADS NAMES libevent_pthreads.a event_pthreads)
message(STATUS "Using libevent_pthreads ${_EVENT_LIB_PTHREADS}")
list(APPEND DRIVER_LIB_LIST ${_EVENT_LIB_PTHREADS})

# GLOG

find_library(_GLOG_LIB NAMES libglog.a glog)
message(STATUS "Using glog ${_GLOG_LIB}")
list(APPEND LIB_LIST ${_GLOG_LIB})

# GFLAGS (grpc is using it as dynamic lib)

set(gflags_STATIC ON)
find_package(gflags CONFIG REQUIRED)
message("-- Using gflags ${gflags_VERSION} libs ${gflags_LIBRARIES}")
list(APPEND LIB_LIST ${gflags_LIBRARIES})

# Protobuf

#pkg_check_modules (protobuf protobuf>=3.5.1)
find_library(_PROTOBUF_LIB NAMES libprotobuf.a protobuf)
message(STATUS "Using protobuf ${_PROTOBUF_LIB}")
list(APPEND LIB_LIST ${_PROTOBUF_LIB})

# Protoc

find_program(_PROTOC_EXEC
          NAMES protoc
          DOC "protoc compiler"
          )
message(STATUS "Using protoc ${_PROTOC_EXEC}")


# CARES

find_library(_CARES_LIB NAMES libcares.a cares)
message(STATUS "Using Cares ${_CARES_LIB}")
list(APPEND LIB_LIST ${_CARES_LIB})

# GPR
find_library(_GPR_LIB NAMES libgpr.a gpr)
message(STATUS "Using GPR ${_GPR_LIB}")
list(APPEND LIB_LIST ${_GPR_LIB})

# GRPC

find_library(_GRPC_LIB NAMES libgrpc.a grpc)
message(STATUS "Using gRPC ${_GRPC_LIB}")
list(APPEND LIB_LIST ${_GRPC_LIB})

# GRPC++

#pkg_check_modules (grpc++ grpc++>=1.2.0)

find_library(_GRPCPP_LIB NAMES libgrpc++.a grpc++)
message(STATUS "Using gRPC++ ${_GRPCPP_LIB}")
list(APPEND LIB_LIST ${_GRPCPP_LIB})

# GRPC++ Plugin

find_program(_GRPCPP_PLUGIN_EXEC
        NAMES grpc_cpp_plugin
        DOC "grpc_cpp_plugin compiler"
        )
message(STATUS "Using gRPC++ Plugin ${_GRPCPP_PLUGIN_EXEC}")

# OpenSSL

if(NOT OPENSSL_ROOT_DIR)
  set(OPENSSL_ROOT_DIR "$ENV{OPENSSL_ROOT_DIR}")
endif()


message("OpenSSL Root Dir: ${OPENSSL_ROOT_DIR}")

set(OPENSSL_STATIC ON)
set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL REQUIRED)

message("OpenSSL Include Dir: ${OPENSSL_INCLUDE_DIR}")
message("OpenSSL Libraries: ${OPENSSL_LIBRARIES}")

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

target_link_libraries(gkvs-cli 
        ${LIB_LIST})

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

message("-- LIB_LIST ${LIB_LIST}")

target_link_libraries(gkvs
	${LIB_LIST}
        ${DRIVER_LIB_LIST})

