use std::{slice::Iter, collections::{HashSet, HashMap}};

use crate::record::{Record, HashedKey};

pub struct MemTable {
    buffer: HashMap<HashedKey, Record>,
    pub references: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable { buffer: HashMap::new(), references: 0 }
    }

    pub fn append(&mut self, record: Record) -> usize {
        if self.buffer.insert(record.hash, record).is_none() {
            // Update reference only if there was no values before 
            self.references += 1;
        }
        self.buffer.len() - 1
    }

    pub fn get(&self, hash: &HashedKey) -> &Record {
        &self.buffer[hash]
    }

    pub fn len(&self) -> usize {
        return self.references
    }

    pub fn iter(&self) -> std::collections::hash_map::Values<[u8; 20], Record> {
        return self.buffer.values().into_iter();
    }

    pub fn truncate(&mut self) {
        self.buffer = HashMap::new();
    }
}
