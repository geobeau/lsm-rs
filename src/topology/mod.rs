use std::{collections::HashMap, hash::Hash, net::IpAddr};

pub const MAX_RANGE: u16 = 2u16.pow(14);

#[derive(PartialEq, Debug, Clone)]
pub struct ShardRange {
    pub start: u16,
    pub end: u16,
}

#[derive(PartialEq, Eq, Debug, Clone, Hash)]
pub struct ReactorMetadata {
    pub id: u8,
    pub ip: IpAddr,
    pub port: u16,
}

#[derive(Clone, Debug)]
pub struct LocalTopology {
    pub reactor: ReactorMetadata,
    pub shards: Vec<ShardRange>,
}

#[derive(Clone, Debug)]
pub struct Topology {
    pub shards_count: u16,
    pub reactor_allocations: HashMap<ReactorMetadata, Vec<ShardRange>>,
}

impl Topology {
    pub fn new_with_reactors(shards_count: u16, reactors: Vec<ReactorMetadata>) -> Topology {
        let mut offset = 0;

        // Ensure 16k is divisible by shards_count
        assert_eq!(MAX_RANGE % shards_count, 0);

        let mut slots = Vec::with_capacity(shards_count as usize);
        let range = MAX_RANGE / shards_count;

        for _ in 0..shards_count {
            slots.push(ShardRange {
                start: offset,
                end: offset + range,
            });
            offset += range
        }

        // Allocate ranges to reactors in round robin fashion
        let offset = 0;
        let mut reactor_allocations = HashMap::with_capacity(reactors.len());
        for reactor in &reactors {
            reactor_allocations.insert(reactor.clone(), Vec::new());
        }

        for slot in slots {
            let reactor = &reactors[offset % reactors.len()];
            let node_slots = reactor_allocations.get_mut(reactor).unwrap();
            node_slots.push(slot);
        }

        Topology {
            shards_count,
            reactor_allocations,
        }
    }
}

/// Align `shard` with the proper slot (slot are determined by the number of shards)
pub fn compute_shard_id(shard: u16, total_shards: u16) -> u16 {
    let multiple = MAX_RANGE / total_shards;
    ((shard + multiple - 1) / multiple) * multiple - multiple
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_even_range_splitting() {
//         let topo = Topology::new_with_shard(1, 1);
//         assert_eq!(topo.shards[&0].range, Range{start: 0, end: MAX_RANGE});

//         let topo = Topology::new_with_shard(2, 1);
//         assert_eq!(topo.shards[&0].range, Range{start: 0, end: MAX_RANGE /2});
//         assert_eq!(topo.shards[&1].range, Range{start: MAX_RANGE/2, end: MAX_RANGE});

//         let topo = Topology::new_with_shard(3, 1);
//         assert_eq!(topo.shards[&0].range, Range{start: 0, end: 5462});
//         assert_eq!(topo.shards[&1].range, Range{start: 5462, end: 10923});
//         assert_eq!(topo.shards[&2].range, Range{start: 10923, end: MAX_RANGE});

//         let topo = Topology::new_with_shard(62, 1);
//         assert_eq!(topo.shards[&0].range, Range{start: 0, end: 265});
//         assert_eq!(topo.shards[&4].range, Range{start: 1060, end: 1325});
//         assert_eq!(topo.shards[&5].range, Range{start: 1325, end: 1590});
//         assert_eq!(topo.shards[&61].range, Range{start: 16120, end: MAX_RANGE});
//     }
// }
