# practical-distributed-systems-project


1. 
    a) Append tags to aerospike database: check if cookie is in database, clip old when adding
; optimistic locking needed: Write policy expect_gen_equal

    b) And publish event tag to kafka

2. Read data of user from database and apply time filtering

3. Another component called "procesor" it uses kafka streams, it takes messages from kafka, and computes aggregate metrics and ocassionaly dumps into aerospike; \
1 thread, At least once
Every 15seconds dump aggregates to database \
useful terms:
    - That can be achieved with a punctuator
    - Batch operations in aerospike
    - Atomic counters
    - Remember about compression of json in databases and kafka, ( in kafka config compression type snappy  and linger_ms_config 5s)


Additional info:
- Machines:
    - Kafka 2
    - Aerospike 5
    - Haproxy 1 (shared with front)
    - Front 2
    - Procesor 1/2

- Kafka replication factor 2
- 2 partitions minimum
- Add  24h retention

- Make aerospike persistent, best to use a few files

- Docker/registry/swarm for CI/CD