#!/bin/bash

echo "Install libs"

sudo apt-get update

sudo apt-get install build-essential autoconf automake libtool pkg-config cmake libc6-dev libssl-dev libevent-dev
sudo apt-get install ncurses-dev liblua5.1-dev

sudo apt-get install libgflags-dev libgtest-dev
sudo apt-get install clang libc++-dev g++

sudo apt-get install libsnappy-dev
