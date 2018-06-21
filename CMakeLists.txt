cmake_minimum_required(VERSION 3.10)

project(gkvs-super C CXX)

include(ExternalProject)

set(CMAKE_CXX_STANDARD 11)
set(GFLAGS_BUILD_STATIC_LIBS ON)
set(gRPC_GFLAGS_PROVIDER package)
set(gRPC_BUILD_CODEGEN ON)

# setup ENV in your bash
set(OPENSSL_ROOT_DIR /usr/local/Cellar/openssl/1.0.2o_1)

include_directories(src)
include_directories("modules/aerospike-client-c/target/Darwin-x86_64/include")
include_directories("modules/aerospike-client-c/modules/common/src/include")

find_library(_AS_LIB aerospike)
message(STATUS "Using Aerospike lib ${_AS_LIB}")

# Builds c-ares project from the git submodule
ExternalProject_Add(c-ares
        PREFIX c-ares
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/modules/grpc/third_party/cares/cares"
        CMAKE_CACHE_ARGS
        -DCARES_SHARED:BOOL=OFF
        -DCARES_STATIC:BOOL=ON
        -DCARES_STATIC_PIC:BOOL=ON
        -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/c-ares
        )

# Builds protobuf project from the git submodule.
ExternalProject_Add(protobuf
        PREFIX protobuf
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/modules/grpc/third_party/protobuf/cmake"
        CMAKE_CACHE_ARGS
        -Dprotobuf_BUILD_TESTS:BOOL=OFF
        -Dprotobuf_WITH_ZLIB:BOOL=OFF
        -Dprotobuf_MSVC_STATIC_RUNTIME:BOOL=OFF
        -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/protobuf
        )

# Builds zlib project from the git submodule.
ExternalProject_Add(zlib
        PREFIX zlib
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/modules/grpc/third_party/zlib"
        CMAKE_CACHE_ARGS
        -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/zlib
        )


# the location where protobuf-config.cmake will be installed varies by platform
if (WIN32)
    set(_FINDPACKAGE_PROTOBUF_CONFIG_DIR "${CMAKE_CURRENT_BINARY_DIR}/protobuf/cmake")
else()
    set(_FINDPACKAGE_PROTOBUF_CONFIG_DIR "${CMAKE_CURRENT_BINARY_DIR}/protobuf/lib/cmake/protobuf")
endif()

# if OPENSSL_ROOT_DIR is set, propagate that hint path to the external projects with OpenSSL dependency.
set(_CMAKE_ARGS_OPENSSL_ROOT_DIR "")
if (OPENSSL_ROOT_DIR)
    set(_CMAKE_ARGS_OPENSSL_ROOT_DIR "-DOPENSSL_ROOT_DIR:PATH=${OPENSSL_ROOT_DIR}")
endif()

# Builds gRPC based on locally checked-out sources and set arguments so that all the dependencies
# are correctly located.
ExternalProject_Add(grpc
        PREFIX grpc
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/modules/grpc"
        CMAKE_CACHE_ARGS
        -DgRPC_INSTALL:BOOL=ON
        -DgRPC_BUILD_TESTS:BOOL=OFF
        -DgRPC_PROTOBUF_PROVIDER:STRING=package
        -DgRPC_PROTOBUF_PACKAGE_TYPE:STRING=CONFIG
        -DProtobuf_DIR:PATH=${_FINDPACKAGE_PROTOBUF_CONFIG_DIR}
        -DgRPC_ZLIB_PROVIDER:STRING=package
        -DZLIB_ROOT:STRING=${CMAKE_CURRENT_BINARY_DIR}/zlib
        -DgRPC_CARES_PROVIDER:STRING=package
        -Dc-ares_DIR:PATH=${CMAKE_CURRENT_BINARY_DIR}/c-ares/lib/cmake/c-ares
        -DgRPC_SSL_PROVIDER:STRING=package
        ${_CMAKE_ARGS_OPENSSL_ROOT_DIR}
        -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/grpc
        DEPENDS c-ares protobuf zlib
        )

ExternalProject_Add(gkvs
        PREFIX gkvs
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src"
        BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/gkvs"
        INSTALL_COMMAND ""
        CMAKE_CACHE_ARGS
        -DProtobuf_DIR:PATH=${_FINDPACKAGE_PROTOBUF_CONFIG_DIR}
        -Dc-ares_DIR:PATH=${CMAKE_CURRENT_BINARY_DIR}/c-ares/lib/cmake/c-ares
        -DZLIB_ROOT:STRING=${CMAKE_CURRENT_BINARY_DIR}/zlib
        ${_CMAKE_ARGS_OPENSSL_ROOT_DIR}
        -DgRPC_DIR:PATH=${CMAKE_CURRENT_BINARY_DIR}/grpc/lib/cmake/grpc
        DEPENDS protobuf grpc
        )

# GRPC

#add_subdirectory(modules/grpc ${CMAKE_CURRENT_BINARY_DIR}/grpc EXCLUDE_FROM_ALL)
#message(STATUS "Using gRPC via add_subdirectory.")

#add_subdirectory(modules/glog ${CMAKE_CURRENT_BINARY_DIR}/glog EXCLUDE_FROM_ALL)
#message(STATUS "Using GLOG via add_subdirectory.")

# After using add_subdirectory, we can now use the grpc targets directly from
# this build.
#set(_PROTOBUF_LIBPROTOBUF libprotobuf)
#set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
#set(_GRPC_GRPCPP grpc++_unsecure)
#set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)
#set(_GFLAGS gflags)
#set(_GLOG glog)

#find_package(OpenSSL REQUIRED)

#message("OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")
#message("OpenSSL libraries: ${OPENSSL_LIBRARIES}")

#include_directories(${OPENSSL_INCLUDE_DIR})
#list(APPEND LIB_LIST ${OPENSSL_LIBRARIES})

#get_filename_component(gkvs_proto "protos/gkvs.proto" ABSOLUTE)
#get_filename_component(gkvs_proto_path "${gkvs_proto}" PATH)

# Generated sources
#set(gkvs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/gkvs.pb.cc")
#set(gkvs_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gkvs.pb.h")
#set(gkvs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/gkvs.grpc.pb.cc")
#set(gkvs_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gkvs.grpc.pb.h")

#add_custom_command(
#        OUTPUT "${gkvs_proto_srcs}" "${gkvs_proto_hdrs}" "${gkvs_grpc_srcs}" "${gkvs_grpc_hdrs}"
#        COMMAND ${_PROTOBUF_PROTOC}
#        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
#        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
#        -I "${gkvs_proto_path}"
#        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
#        "${gkvs_proto}"
#        DEPENDS "${gkvs_proto}")

# Include generated *.pb.h files
#include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# Build client

#add_executable(gkvs_client
#        "src/gkvs_client.cc"
#        ${gkvs_proto_srcs}
#        ${gkvs_grpc_srcs})

#target_link_libraries(gkvs_client
#        PRIVATE ${_GRPC_GRPCPP}
#        PRIVATE ${_PROTOBUF_LIBPROTOBUF}
#        PRIVATE ${_GLOG}
#        PRIVATE ${_GFLAGS})

# Build server

#add_executable(gkvs_server2
#        "src/gkvs_server.cc"
#        "src/helper.cc"
#        "src/as_driver.cc"
#        "src/as_driver_tests.cc"
#        "src/as_helper.cc"
#        "src/crypto.cc"
#        ${gkvs_proto_srcs}
#        ${gkvs_grpc_srcs})

#target_link_libraries(gkvs_server
#        ${_GRPC_GRPCPP}
#        ${_PROTOBUF_LIBPROTOBUF}
#        ${_GLOG}
#        ${LIB_LIST}
#        ${_AS_LIB})


#add_executable(gkvs
#        src/gkvs.grpc.pb.h
#        src/gkvs.pb.h
#        src/gkvs_client.cc
#        src/gkvs_server.cc
#        src/helper.cc
#        src/helper.h
#        src/driver.h
#        src/as_driver.cc
#        src/as_driver.h
#        src/as_driver_tests.cc
#        src/crypto.cc
#        src/crypto.h
#        src/as_helper.cc)
