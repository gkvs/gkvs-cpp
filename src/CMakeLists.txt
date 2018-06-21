cmake_minimum_required(VERSION 3.10)

project(gkvs C CXX)

set(CMAKE_CXX_STANDARD 11)

include_directories("../modules/aerospike-client-c/modules/common/src/include")

find_library(_AS_LIB
        NAMES libaerospike.a aerospike
        HINTS ../modules/aerospike-client-c/target)
message(STATUS "Using Aerospike lib ${_AS_LIB}")

find_library(_EVENT libevent.a event)
message(STATUS "Using libevent ${_EVENT}")

find_library(_EVENT_PTHREADS libevent_pthreads.a event_pthreads)
message(STATUS "Using libevent_pthreads ${_EVENT_PTHREADS}")



# GRPC

#add_subdirectory(modules/grpc ${CMAKE_CURRENT_BINARY_DIR}/grpc EXCLUDE_FROM_ALL)
#message(STATUS "Using gRPC via add_subdirectory.")

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
#find_package(glog CONFIG REQUIRED)


#set(_PROTOBUF_LIBPROTOBUF libprotobuf)
#set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
#set(_GRPC_GRPCPP grpc++_unsecure)
#set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)
#set(_GFLAGS gflags)
#set(_GLOG glog)

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

add_executable(gkvs_client
        "gkvs_client.cc"
        ${gkvs_proto_srcs}
        ${gkvs_grpc_srcs})

target_link_libraries(gkvs_client
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        glog
        gflags)

# Build server

add_executable(gkvs_server
        "gkvs_server.cc"
        "helper.cc"
        "as_driver.cc"
        "as_driver_tests.cc"
        "as_helper.cc"
        "crypto.cc"
        ${gkvs_proto_srcs}
        ${gkvs_grpc_srcs})

target_link_libraries(gkvs_server
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        glog
        libaerospike.a
        ${LIB_LIST}
        ${_EVENT}
        ${_EVENT_PTHREADS})

