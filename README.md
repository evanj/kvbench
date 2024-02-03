# Key/Value Bench

Yet another benchmark for key/value stores. There are lots out there that are probably better than this one. This is mostly an excuse to learn Rust, but also to get some performance numbers.


## Included backends

* In memory HashMap
* In memory BTreeMap
* Redis

The benchmark will consume 100% CPU while executing the in-memory backends. It will consume slightly more than 1 CPU core while running a single-threaded Redis benchmark.


## Example results

1M 64-bit (8 byte) key/values: 16 MiB of raw data. HashMap: 3M reads/sec; BTree: 1.2M reads/sec; Redis: 83k reads/sec

BTree:
    * load: filled in 369.020403ms ; 2709877.3 keys/sec
    * memory: ~154 MiB
    * read: 25070868 requests in 20.000000691s; 1253543.357 requests/sec
HashMap: 
    * load: filled in 377.97916ms ; 2645648.5 keys/sec
    * memory: ~161 MiB
    * read: 60347709 requests in 20.000000434s; 3017385.385 requests/sec
Redis
    * load: filled in 12.435092877s ; 80417.6 keys/sec
    * memory: ~100 MiB
    * read: 1665108 requests in 20.000016961s; 83255.329 requests/sec


## Redis backend

The unit tests will start and stop a localhost Redis server. The ubuntu package `redis-server` will need to be installed.
