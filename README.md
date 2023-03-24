# lsm-rs

Lsm-rs is a database engine that aims at being a high performance key/value store.
The main features:
- NVMe ssd first: data structures have been chosen to leverage the properties of NVMe ssd to the maximum (at the expense of hdd)
- Read optimized: it aims at having low latency (<1ms, 99% of the time) while handling hundreds of thousands of reads per second
- Support expiration
- Maybe support eviction


It should feature:
- shart per core model
- asynchronous I/O with io_uring

## Storage architecture

### Indexing

The goal is to reach each record on disk at maximum 1 I/O.
To achieve this, we need to store the index in memory. To have a predictable size of the index in memory (i.e. not depend on size
of the key), we hash the key. We don't want to handle collisions either, so we need a strong hashing function with high enough
entropy. It seems the minimum collisions-less hash digest is 160 bits (20 bytes) and we are using SHA-1.

Each entry of the index contains:
- timestamp of the latest update (for consistency and expiration)
- pointer to the data
- hash of the key
- size of the record

The underlying datastructure of the index is hashmap but it will change because:
- hashmap have a memory overhead
- hashmap are expensive to resize
So it will probably evolve to a btreemap.

### Memtable & Disktable

#### Memtable

All writes are added to the memtable. This memtable is flushed to disk once it's full. It is flushed as a disktable
In future version, it will be flushed at regular interval.

The current underlying datastructure is a hashmap to have a very easy way to handle updates (when a record is updated but
not yet flushed to disktable, we can juste replace it and same some ressources). 

#### Disktable 

Disktable is a file containing the records. It is not sorted (hence not an SSTable).

#### Compaction/Reclaim

Disktable are reference counted, once they go under a certain ratio, they are marked for Reclamation. References are decremented
everytime we update a record (in a new disktable), delete a record and expire a record.
Reclamation read the full disktable, keep only in-use data and append the remaining data to the memtable.
