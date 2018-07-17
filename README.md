# gkvs
Generic Key-Value Storage

### Vision
* Federated tables (external clusters: Aerospike, Redis)
* Internal tables (engine: RocksDB)
* Zero-config
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

Server node receives configuration commands by network and execute them immediatelly.
Raft consensus algorithm is using to distribute them in a pool.

All succesfully executed commands will be appended to the command log and written to text file on disk (editable).
On startup node will execute all of them.

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

#### RocksDB

RocksDB is the key-value engine, not a cluster. GKVS manage data distribution for RocksDB.
There are two major modes of data distribution in the pool: 1) data replicated 2) data partitioned

All tables in RocksDB are placed in a single database with a key-prefix. Default column factory is using
for replicated data, all other column families are having name of the Bucket.

At the time of creation of rocksdb cluster we define number of buckets. By default value is 1023.
This number likely to be prime, in order to make balancing more efficient.

In client API we setup partitionKey if we want to split data between nodes (buckets). In other case
data will be replicated and placed to default column factory.

Each node that joins the pool share it's disk space and load buckets from other nodes.
After coming to ready state it serves the traffic.

In SYSTEM database (that has only default column family) we keep all data replicated and store
information about other nodes and external clusters.
