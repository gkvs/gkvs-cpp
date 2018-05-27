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

Operations:
* Get - gets value by key and version, if not found return null
* Put - puts not null value by key (can not put null values)
* CompareAndPut - puts not null value by key and checks version
* Remove - removes value by key




