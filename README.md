# gkvs
Generic Key-Value Storage

### Vision
* Federated tables
* Zero-config
* Written on C/C++ (for 5 nines and low latency)
* Network interface GRPC (Sync and Async)
* All client languages are supported
* Automatic load-balancer
* Failover support
* Multi datacenter support
* All traffic is encrypted by SSL

### API Design

* Key is byte array NOT NULL
* Value is byte array NOT NULL

SLA Operations:
* Get - gets value (and version) by key, if not found return null
* Put - puts not null value by key (can not put null values)
* CompareAndPut - puts not null value by key and checks version
* Remove - removes value by key

SLA Multi Operations:
* multiGet - gets values in a batch
* getAll - gets values as stream
* putAll - puts values as stream
* removeAll - removes values as stream

NON SQL Operations:
* Scan - query all key-value pairs with some conditions, supports bucket selection ( buckerNumber = hash(key) % n)

PIT (Point In Time) support if configured:
* Stores all values as Map<timestamp, value> map
* Put - calls map.put(timestamp, value)
* Remove - calls map.remove(timestamp, value)
* Get - calls map.filter(pit <= request_pit).max(pit), if request_pit not defined, then pit = Current.timestamp();
* Scan - the same as Get

### Build

Build and install in local system:
* Protobuf 3.5.1
* GRPC 1.2.0
* Aerospike Client
* Redis
* RocksDB
* LuaJIT
* Msgpack
* LZ4, ZSDT, Snappy, BZ2, Z

Build the project:
```
make
```

Run sync_server
```
./src/gkvs_server
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
