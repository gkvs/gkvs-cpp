#!/bin/bash

echo "Install libs"

sudo xcode-select --install

brew install autoconf automake libtool shtool

brew install gflags

brew install protobuf grpc glog

brew install hiredis

brew install rocksdb

brew install luajit

brew install cmake
