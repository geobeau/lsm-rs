use std::collections::HashMap;

const MAX_RANGE: u16 = 2u16.pow(14);

#[derive(PartialEq, Debug, Clone)]
pub struct Range {
    pub start: u16,
    pub end: u16,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Shard {
    pub range: Range,
    pub port: u16,
    pub id: u8,
}

#[derive(Clone, Debug)]
pub struct Topology {
    pub shards: HashMap<u8, Shard>
}

impl Topology {
    pub fn new_with_shard(nr_shards: u8, base_port: u16) -> Topology {
        let range = MAX_RANGE / nr_shards as u16;
        let mut shards = HashMap::with_capacity(nr_shards as usize);
        let mut offset = 0;

        let restartder = MAX_RANGE % nr_shards as u16;

        let mut chunks = vec![range; nr_shards as usize];
        for i in 0..restartder {
            chunks[i as usize] += 1;
        }

        for i in 0..chunks.len() {
            let range = Range {
                start: offset,
                end: offset+chunks[i],
            };

            shards.insert(i as u8, Shard { range, port: base_port + i as u16, id: i as u8 });
            offset = offset+chunks[i];
        }

        return Topology{shards};
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_even_range_splitting() {
        let topo = Topology::new_with_shard(1, 1);
        assert_eq!(topo.shards[&0].range, Range{start: 0, end: MAX_RANGE});

        let topo = Topology::new_with_shard(2, 1);
        assert_eq!(topo.shards[&0].range, Range{start: 0, end: MAX_RANGE /2});
        assert_eq!(topo.shards[&1].range, Range{start: MAX_RANGE/2, end: MAX_RANGE});

        let topo = Topology::new_with_shard(3, 1);
        assert_eq!(topo.shards[&0].range, Range{start: 0, end: 5462});
        assert_eq!(topo.shards[&1].range, Range{start: 5462, end: 10923});
        assert_eq!(topo.shards[&2].range, Range{start: 10923, end: MAX_RANGE});
    
        let topo = Topology::new_with_shard(62, 1);
        assert_eq!(topo.shards[&0].range, Range{start: 0, end: 265});
        assert_eq!(topo.shards[&4].range, Range{start: 1060, end: 1325});
        assert_eq!(topo.shards[&5].range, Range{start: 1325, end: 1590});
        assert_eq!(topo.shards[&61].range, Range{start: 16120, end: MAX_RANGE});
    }
}