use std::{slice::Iter, collections::{HashSet, HashMap}};

use crate::record::{Record, HashedKey};

pub struct MemTable {
    buffer: HashMap<HashedKey, Record>,
    /// Number of references on it from the index
    pub references: usize,
    /// Number of bytes of data added to it
    bytes: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable { buffer: HashMap::new(), references: 0, bytes: 0 }
    }

    pub fn append(&mut self, record: Record) {
        let size = record.size_of();
        match self.buffer.insert(record.hash, record) {
            Some(old) => {
                self.bytes += size - old.size_of();
            },
            None => {
                self.references += 1;
                self.bytes += size;
            },
        }           
    }

    pub fn get(&self, hash: &HashedKey) -> &Record {
        &self.buffer[hash]
    }

    pub fn len(&self) -> usize {
        return self.buffer.len()
    }

    pub fn references(&self) -> usize {
        return self.references
    }

    pub fn iter(&self) -> std::collections::hash_map::Values<[u8; 20], Record> {
        return self.buffer.values().into_iter();
    }

    pub fn truncate(&mut self) {
        self.buffer = HashMap::new();
    }
}
