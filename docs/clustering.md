# Terminology

## Cluster

Cluster is a set of nodes working together to provide the data split in shards.

## Reactor

A reactor is part of a node: each reactor is mostly independant from each other. They
own different shards. They only share the heartbeat and clustering information. 

## Slots

Slots represent a sub-part of the key space. They follow the redis definition of shards:
- Up to 16384 shards
- Hashing function is crc16 % 16384

## Shards

Shards is the coalescing of slots in ranges: it is not practical to have to many shards
as it increases overhead. So we use range of slots as shards.
