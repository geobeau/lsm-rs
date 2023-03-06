use std::{collections::HashMap, mem, ops::DerefMut};

use super::{RecordMetadata, HashedKey};

pub struct Index {
    record_vec: Vec<RecordMetadata>,
    kvs: HashMap<HashedKey, usize>
}

impl Index {
    pub fn new() -> Index {
        return Index {
            record_vec: Vec::new(),
            kvs: HashMap::new(),
        }
    }

    /// Update the index with new metadata
    /// If there was already a record in the index with older metadata
    /// return it and apply the new one.
    pub fn update(&mut self, meta: RecordMetadata) -> Option<RecordMetadata> {
        match self.kvs.get(&meta.hash) {
            Some(idx) => {
                let old = self.record_vec[*idx].clone();
                self.record_vec[*idx] = meta;
                return Some(old)
            },
            None => {
                let hash = meta.hash;
                self.record_vec.push(meta);
                let idx = self.record_vec.len() - 1;
                self.kvs.insert(hash, idx);
                return None
            },
        }
    }

    pub fn get(&self, hash: HashedKey) -> Option<&RecordMetadata> {
        match self.kvs.get(&hash) {
            Some(idx) => {
                return Some(&self.record_vec[*idx])
            },
            None => {
                return None
            },
        }
    }
}