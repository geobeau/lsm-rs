# lsm-rs

Lsm-rs is a persisted Key/Value Store that is partially compatible with memcached

It works with a shard per core model (like Scylla).
At the moment it's using 2 implementations:
- with bytedance/monoio
- with datadog/gloomio

Gloomio implem is able to scale to 600k QPS in 4 cpu mode and 300k QPS in one cpu mode.
Monoio implem is able to scale 330k QPS but multi cpu is not yet implemented


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
