use std::{
    cell::RefCell,
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
};

use super::{HashedKey, RecordMetadata};

#[derive(Debug)]
pub struct Index {
    kvs: RefCell<HashMap<HashedKey, RecordMetadata>>,
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Index {
    pub fn new() -> Index {
        Index {
            kvs: RefCell::from(HashMap::new()),
        }
    }

    /// Update the index with new metadata
    /// If there was already a record in the index with older metadata (timestamp)
    /// return it and apply the new one.
    pub fn update(&self, meta: RecordMetadata) -> Option<RecordMetadata> {
        match self.kvs.borrow_mut().entry(meta.hash) {
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

    pub fn delete(&self, meta: &RecordMetadata) {
        self.kvs.borrow_mut().remove(&meta.hash);
    }

    pub fn get(&self, hash: HashedKey) -> Option<RecordMetadata> {
        self.kvs.borrow().get(&hash).cloned()
    }

    pub fn truncate(&self) {
        self.kvs.borrow_mut().clear();
    }

    pub fn len(&self) -> usize {
        self.kvs.borrow().len()
    }
}
