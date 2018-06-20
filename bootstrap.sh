#!/bin/bash


git submodule update --init
pushd modules
pushd aerospike-client-c
git submodule update --init
make EVENT_LIB=libevent
popd
popd
make
