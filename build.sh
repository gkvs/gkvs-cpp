#!/bin/bash

echo "Build GKVS"

rm -rf build
mkdir build
cd build
cmake ..
make


