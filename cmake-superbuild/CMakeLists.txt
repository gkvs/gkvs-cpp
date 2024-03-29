cmake_minimum_required(VERSION 3.5)

project(gkvs-super C CXX)

include(ExternalProject)

set(CMAKE_CXX_STANDARD 11)
set(_GKVS_SUPERBUILD ON)


# Builds c-ares project from the git submodule
ExternalProject_Add(c-ares
        PREFIX c-ares
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../modules/grpc/third_party/cares/cares"
        CMAKE_CACHE_ARGS
        -DCARES_SHARED:BOOL=OFF
        -DCARES_STATIC:BOOL=ON
        -DCARES_STATIC_PIC:BOOL=ON
        -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/c-ares
        )

# Builds protobuf project from the git submodule.
ExternalProject_Add(protobuf
        PREFIX protobuf
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../modules/grpc/third_party/protobuf/cmake"
        CMAKE_CACHE_ARGS
        -Dprotobuf_BUILD_TESTS:BOOL=OFF
        -Dprotobuf_WITH_ZLIB:BOOL=OFF
        -Dprotobuf_MSVC_STATIC_RUNTIME:BOOL=OFF
        -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/protobuf
        )

# Builds zlib project from the git submodule.
ExternalProject_Add(zlib
        PREFIX zlib
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../modules/grpc/third_party/zlib"
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
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../modules/grpc"
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

# Builds msgpack-c project from the git submodule
ExternalProject_Add(msgpack
        PREFIX msgpack
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../modules/msgpack-c"
        CMAKE_CACHE_ARGS
        -DMSGPACK_CXX11:BOOL=ON
        -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/msgpack
        )

# Build gkvs as external project, now all deps are available in find_package
ExternalProject_Add(gkvs
        PREFIX gkvs
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../src"
        BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/gkvs"
        INSTALL_COMMAND ""
        CMAKE_CACHE_ARGS
        -DProtobuf_DIR:PATH=${_FINDPACKAGE_PROTOBUF_CONFIG_DIR}
        -Dc-ares_DIR:PATH=${CMAKE_CURRENT_BINARY_DIR}/c-ares/lib/cmake/c-ares
        -DZLIB_ROOT:STRING=${CMAKE_CURRENT_BINARY_DIR}/zlib
        ${_CMAKE_ARGS_OPENSSL_ROOT_DIR}
        -DgRPC_DIR:PATH=${CMAKE_CURRENT_BINARY_DIR}/grpc/lib/cmake/grpc
        DEPENDS protobuf grpc msgpack
        )

