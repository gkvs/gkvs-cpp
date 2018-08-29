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


git clone http://luajit.org/git/luajit-2.0.git
cd luajit-2.0
git checkout tags/v2.0.5
make
sudo make install