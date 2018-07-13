# gkvs
Generic Key-Value Storage

### Vision
* Federated tables (external clusters: Aerospike, Redis)
* Internal tables (engine: RocksDB)
* Zero-config (everything configured by Lua scripts, supports remote config)
* Server written on C/C++, client on Java
* Raft consensus algorithm to manage nodes
* Protocol on gRPC/Protobuf
* Schema-less, all data in msgpack (binary JSON)
* Automatic load-balancer
* Failover support
* Multi datacenter support
* No plain traffic, always SSL

### API Design

* Key is byte array or it's hash, NOT NULL
* Value is byte array in msgpack, NOT NULL

Basic Operations:
* get - gets value (and metadata) by key, if not found return null (not error)
* put - puts value by key
* compareAndPut - puts value by key only if version match
* remove - removes value by key

Bulk Operations:
* multiGet - gets values in a batch

Stream Operations:
* getAll - gets values as stream
* putAll - puts values as stream
* removeAll - removes values as stream
* scan - query all key-value pairs with some conditions

### Errors 

All errors are passthrough to client.

### Build

Required libraries:
* Protobuf 3.5.1
* GRPC 1.2.0
* Aerospike Client
* Redis
* RocksDB
* LuaJIT
* Msgpack
* LZ4, ZSDT, Snappy, BZ2, Z
* OpenSSL

Build:
```
make
```

Run:
```
./src/gkvs_server redis.conf
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
