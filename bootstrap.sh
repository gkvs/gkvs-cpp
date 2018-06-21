#!/bin/bash

echo "Init modules"

git submodule update --init --recursive

echo "Build Aerospike Client"

pushd modules
pushd aerospike-client-c
make EVENT_LIB=libevent
popd
popd

echo "Build GKVS"

mkdir build
cd build
cmake ..

