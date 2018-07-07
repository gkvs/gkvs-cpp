#!/bin/bash

echo "Init modules"

git submodule update --init --recursive

echo "Build Redis Client"

pushd modules
pushd hiredis
make
popd
popd

echo "Build Aerospike Client"

pushd modules
pushd aerospike-client-c
make EVENT_LIB=libevent
popd
popd

echo "Build RocksDB"

pushd modules
pushd rocksdb
make OPT=-DSNAPPY
popd
popd
