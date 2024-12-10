use std::{collections::HashMap, hash::Hash, net::IpAddr, vec};

const MAX_RANGE: u16 = 2u16.pow(14);

#[derive(PartialEq, Debug, Clone)]
pub struct Range {
    pub start: u16,
    pub end: u16,
}

#[derive(PartialEq, Eq, Debug, Clone, Hash)]
pub struct Reactor {
    pub ip: IpAddr,
    pub port: u16,
}


#[derive(Clone, Debug)]
pub struct ClusteredReactor {
    pub reactor: Reactor,
    pub ranges: Vec<Range>,
}

#[derive(Clone, Debug)]
pub struct Cluster {
    pub shard_count: u16,
    pub reactor_allocations: HashMap<Reactor, Vec<Range>>,
}

impl Cluster {
    pub fn new_with_reactors(shard_count: u16, reactors: Vec<Reactor>) -> Cluster {
        let range = MAX_RANGE / shard_count as u16;
        let mut offset = 0;

        // Ensure 16k is divisible by shard_count
        assert_eq!(MAX_RANGE % shard_count, 0);

        let mut ranges = Vec::with_capacity(shard_count as usize);
        let range = MAX_RANGE / shard_count;

        for i in 0..shard_count {
            ranges.push(Range{ start: offset, end: offset + range });
            offset += range
        }

        // Allocate ranges to reactors in round robin fashion
        let mut offset = 0;
        let mut reactor_allocations = HashMap::with_capacity(reactors.len());
        for reactor in &reactors {
            reactor_allocations.insert(reactor.clone(), Vec::new());
        }


        for range in ranges {
            let reactor = &reactors[offset % reactors.len()];
            let nodeRanges = reactor_allocations.get_mut(reactor).unwrap();
            nodeRanges.push(range);
        }


        Cluster {
            shard_count,
            reactor_allocations
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_even_range_splitting() {
//         let topo = Cluster::new_with_shard(1, 1);
//         assert_eq!(topo.shards[&0].range, Range{start: 0, end: MAX_RANGE});

//         let topo = Cluster::new_with_shard(2, 1);
//         assert_eq!(topo.shards[&0].range, Range{start: 0, end: MAX_RANGE /2});
//         assert_eq!(topo.shards[&1].range, Range{start: MAX_RANGE/2, end: MAX_RANGE});

//         let topo = Cluster::new_with_shard(3, 1);
//         assert_eq!(topo.shards[&0].range, Range{start: 0, end: 5462});
//         assert_eq!(topo.shards[&1].range, Range{start: 5462, end: 10923});
//         assert_eq!(topo.shards[&2].range, Range{start: 10923, end: MAX_RANGE});
    
//         let topo = Cluster::new_with_shard(62, 1);
//         assert_eq!(topo.shards[&0].range, Range{start: 0, end: 265});
//         assert_eq!(topo.shards[&4].range, Range{start: 1060, end: 1325});
//         assert_eq!(topo.shards[&5].range, Range{start: 1325, end: 1590});
//         assert_eq!(topo.shards[&61].range, Range{start: 16120, end: MAX_RANGE});
//     }
// }