#!/bin/bash


git submodule update --init --recursive
pushd modules
pushd aerospike-client-c
make EVENT_LIB=libevent
popd
popd

