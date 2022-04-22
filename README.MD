# Freya

is a distributed, fault-tolerant, linearizable key-value storage based on
1. Raft ```https://github.com/lni/dragonboat``` 
2. Badger ```https://github.com/dgraph-io/badger```

## How to run 

```bash
./start-cluster.sh
```
will start a 3 node local cluster.

## Requests to get up and running
1. Add an K/V entry
```
curl --location --request POST 'localhost:8081/put' \
--header 'Content-Type: application/json' \
--data-raw '{
    "k": ["hello"],
    "v": ["value"]
}'
```

2. Retrieve an K/V entry
```
curl --location --request GET 'localhost:8082/get' \
--header 'Content-Type: application/json' \
--data-raw '{
    "k": ["hello"]
}'
```

### TODO
1. Maximize throughput by using multi-raft
2. Create gRPC API
3. Lower overall allocations
4. Create benchmark suite
5. Run 24/7 stress test
