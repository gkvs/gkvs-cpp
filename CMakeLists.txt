cmake_minimum_required(VERSION 3.10)
project(gkvs)

set(CMAKE_CXX_STANDARD 11)

include_directories(src)

add_executable(gkvs
        src/gkvs.grpc.pb.h
        src/gkvs.pb.h
        src/gkvs_client.cc
        src/gkvs_server.cc
        src/helper.cc
        src/helper.h src/driver.h src/as_driver.cc src/as_driver.h)
