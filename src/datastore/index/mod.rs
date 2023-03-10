use std::collections::{
    hash_map::Entry::{Occupied, Vacant},
    HashMap,
};

use super::{HashedKey, RecordMetadata};

#[derive(Debug)]
pub struct Index {
    kvs: HashMap<HashedKey, RecordMetadata>,
}

impl Index {
    pub fn new() -> Index {
        Index { kvs: HashMap::new() }
    }

    /// Update the index with new metadata
    /// If there was already a record in the index with older metadata (timestamp)
    /// return it and apply the new one.
    pub fn update(&mut self, meta: RecordMetadata) -> Option<RecordMetadata> {
        match self.kvs.entry(meta.hash) {
            Occupied(mut entry) => {
                let old = entry.get();
                match meta.timestamp.cmp(&old.timestamp) {
                    // If the new record is older, return it as older
                    std::cmp::Ordering::Less => Some(meta),
                    _ => Some(entry.insert(meta)),
                }
            }
            Vacant(vacant) => {
                vacant.insert(meta);
                None
            }
        }
    }

    pub fn delete(&mut self, meta: &RecordMetadata) {
        self.kvs.remove(&meta.hash);
    }

    pub fn get(&self, hash: HashedKey) -> Option<&RecordMetadata> {
        match self.kvs.get(&hash) {
            Some(r) => Some(r),
            None => None,
        }
    }

    pub fn truncate(&mut self) {
        self.kvs.clear();
    }

    pub fn len(&self) -> usize {
        self.kvs.len()
    }
}
