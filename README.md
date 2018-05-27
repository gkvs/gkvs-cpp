# gkvs
Generic Key-Value Store

### Vision
* Federated (forward to Aerospike and Redis) and own tables (probably RocksDB)
* Zero-config (send all configs by GRPC)
* Written on C/C++ (for 5 nines and low latency)
* Network interface GRPC (Sync and Async)
* All data available by HTTP2 (GRPC), all client languages are supported
* Automatic load-balancer working on Paxos algorithm
* Failover support
* Multi datacenter support (MDC, Zones)


### API Design

* Key is a byte array NOT NULL
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
