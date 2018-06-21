#!/bin/bash

echo "Install libs"

sudo xcode-select --install

brew install autoconf automake libtool shtool

brew install gflags

brew install cmake
