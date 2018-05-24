cmake_minimum_required(VERSION 3.10)
project(gkvs)

set(CMAKE_CXX_STANDARD 11)

include_directories(src)
include_directories("/usr/local/Cellar/openssl/1.0.2o_1/include")
include_directories("modules/aerospike-common/src/include")

add_executable(gkvs
        src/gkvs.grpc.pb.h
        src/gkvs.pb.h
        src/gkvs_client.cc
        src/gkvs_server.cc
        src/helper.cc
        src/helper.h src/driver.h src/as_driver.cc src/as_driver.h src/as_driver_tests.cc src/crypto.cc src/crypto.h src/as_helper.cc)
