# gkvs

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

### Build

Required libraries:
* Protobuf 3.5.1
* GRPC 1.2.0
* Aerospike Client
* Redis
* LuaJIT
* Msgpack
* LZ4, ZSDT, Snappy, BZ2, Z
* OpenSSL

### Build

Legacy make:
```
make
```

General cmake:
```
mkdir build
cd build
cmake ..
make
```

Run:
```
./src/gkvs_server redis1.conf
```

### Configure

GKVS supports config scripts written in LUA language
```

add_cluster("redis1", "redis", { host = "127.0.0.1", port = 6379 } );

add_table("test", "redis1", { ttl = 100 } );

add_view("TEST", "redis1", "test");

```

### Build

Install OpenSSL and define its location, example

```
brew install openssl
export OPENSSL_ROOT_DIR=/usr/local/Cellar/openssl/1.0.2o_1
```

Install libs
```
sudo apt-get install pkg-config libevent-dev
```

Fetch modules in gkvs
```
git submodule update --init --recursive
```

Build Aerospike Client
```
pushd modules
pushd aerospike-client-c
make EVENT_LIB=libevent
popd
popd
```

Build GKVS
```
mkdir build
pushd build
cmake ..
popd
```


#### Brew and package config

brew openssl requirement in env (with make build)
```
export PKG_CONFIG_PATH=/usr/local/opt/openssl/lib/pkgconfig
```


