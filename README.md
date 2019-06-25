# Deprecated

The whole GKVS project was deprecated, please use https://github.com/shvid/datafire

# gkvs-cpp

Server Node written on C++, keep it as legacy code. The actual server node will be in GoLang.

Generic Key-Value Storage

Universal API to access data in multiple key-value databases (proxy and state-less)

### Supported Databases
* Redis
* Aerospike

### Coming databases
* MongoDB
* FoundationDB

### Vision
* Zero-config
* Server written on C/C++, client on Java
* Protocol on gRPC/Protobuf
* Schema-less, all data in MessagePack (aka binary JSON)
* Automatic load-balancer could work in front of GKVS nodes (must support gRPC protocol)
* Supports SSL

### API Design

* Key is byte array or crypto hash, NOT NULL
* Value is byte array in MessagePack, NOT NULL

Basic Operations:
* get - gets value (with/only metadata) by key. If not found return null.
* put - puts value by key
* compareAndSet - puts value by key only if version match
* remove - removes value by key

Bulk Operations:
* multiGet - gets multi entries in one request

Stream Operations:
* getAll - gets values as stream
* putAll - puts values as stream
* removeAll - removes values as stream
* scan - query key-values as stream

### Errors 

All errors are pass-through to client.

### Dependencies

Required libraries:
* Protobuf 3.5.1
* GRPC++ 1.2.0
* Aerospike Client C
* Hiredis
* LuaJIT 2.0.5
* Msgpack 3.0 and upper
* LibZ
* OpenSSL

### Quick Start

* Install Build Essential
```
# Ubuntu
sudo apt-get install build-essential cmake pkg-config autoconf automake libtool
sudo apt-get install clang libc++-dev g++ libc6-dev ncurses-dev

# MacOs
sudo xcode-select --install
brew install autoconf automake libtool shtool cmake
```

* Install OpenSSL
```
# Ubuntu
sudo apt-get install libssl-dev

# MacOs
brew install openssl
```

* Install LibEvent (needed for Aerospike Client)
```
# Ubuntu
sudo apt-get install libevent-dev

# MacOs
brew install libevent
```

* Install gflags (needed for GRPC++ and GKVS)
```
# Ubuntu
sudo apt-get install libgflags-dev

# MacOs
brew install gflags
```

* Install LuaJit (needed by GKVS)
```
# Ubuntu
git clone http://luajit.org/git/luajit-2.0.git
cd luajit-2.0
git checkout tags/v2.0.5
make
sudo make install

# MacOs
brew install luajit
```

* Install golang (needed to build GRPC)
```
# Ubuntu
sudo apt-get install golang

# MacOs
brew install golang
```

* Checkout GKVS and sub-modules
```
git clone https://github.com/gkvs/gkvs
cd gkvs
git submodule update --init --recursive
```

* Install SubModules for MacOs
```
brew install hiredis
brew install protobuf grpc glog
```

* Build SubModules Manually (recommended)
```

# Build MsgPack
cd modules/msgpack-c
mkdir build
cd build
cmake ..
make
# optionally - sudo make install
cd ../../..

# Build gRPC
cd modules/grpc
mkdir build
cd build
cmake ..
make
cd ../../..

# Build hiredis
cd modules/hiredis
make
# optionally - sudo make install
cd ../..

# Build Aerospike
cd modules/aerospike-client-c
make EVENT_LIB=libevent
cd ../..

# Build GKVS
cd gkvs
mkdir build
cd build
cmake ..
make
```

### Run

GKVS reads LUA script on startup and initialize itself

```
# Must have local Redis running
gkvs ../example/redis1-lua.conf

# Must have local Aerospike running
gkvs ../example/as1-lua.conf
```

### Configure

GKVS supports config scripts written in LUA language
This is an example gkvs-lua.conf
```

add_cluster("redis1", "redis", { host = "127.0.0.1", port = 6379 } );

add_table("test", "redis1", { ttl = 100 } );

add_view("TEST", "redis1", "test");

```
