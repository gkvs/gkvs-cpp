# gkvs
Generic Key-Value Service

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
* GRPC
* Aerospike Client

Build the project:
```
make
```

Run sync_server
```
./src/gkvs_server
```

### Client

Client commands to configure service:
```
ADD CLUSTER as1a CONFIG @as1a.json;

ADD CLUSTER as1b CONFIG @as1b.json;

ADD CLUSTER as1 CONFIG ‘{ “active”: “as1a”, “standby”: “as1b”, “driver”: “failover”}’;

ADD TABLE as1.test CONFIG @test.json;

DROP TABLE as1.test;

// only views are available for clients
ADD VIEW test CONFIG '{"type": "simple", "table": "as1.test"}';
DROP VIEW test;

SHOW VIEWS;
SHOW TABLES;
SHOW CLUSTERS;
SHOW USERS;
SHOW RULES;
SHOW MOVES;
SHOW ZONES;
SHOW PIPELINES;

ADD USER alex CONFIG @alex.json;
DROP USER alex;

// rules are working only on views
ADD RULE alex_rule1 rwd 'test'; 
DROP RULE alex_rule1;

ADD MOVE move1 CONFIG '{ "from": "as1.test", "to": "as2.test" }';
DROP MOVE move1;

ADD ZONE us_w CONFIG '{"zone": "us_w", "seed": "5.5.5.5:4040"}';
DROP ZONE us_w;

ADD PIPELINE pipe1 CONFIG '';
DROP PIPLELINE pipe1;

```

