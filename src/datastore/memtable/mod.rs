use std::collections::HashMap;

use crate::record::{HashedKey, Record};

pub struct MemTable {
    buffer: HashMap<HashedKey, Record>,
    /// Number of references on it from the index
    pub references: usize,
    /// Number of bytes of data added to it
    pub bytes: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            buffer: HashMap::new(),
            references: 0,
            bytes: 0,
        }
    }

    pub fn append(&mut self, record: Record) {
        let size = record.size_of();
        if let Some(old) = self.buffer.insert(record.hash, record) {
            self.bytes -= old.size_of();
        }
        self.references += 1;
        self.bytes += size;
    }

    pub fn get(&self, hash: &HashedKey) -> &Record {
        &self.buffer[hash]
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn references(&self) -> usize {
        self.references
    }

    pub fn iter(&self) -> std::collections::hash_map::Values<[u8; 20], Record> {
        self.buffer.values()
    }

    pub fn truncate(&mut self) {
        self.buffer = HashMap::new();
        self.bytes = 0;
        self.references = 0;
    }
}
