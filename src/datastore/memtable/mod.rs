use std::collections::HashMap;

use crate::record::HashedKey;

use super::MemtableEntry;

pub struct MemTable {
    buffer: HashMap<HashedKey, MemtableEntry>,
    /// Number of references on it from the index
    pub references: usize,
    /// Number of bytes of data added to it
    bytes: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            buffer: HashMap::new(),
            references: 0,
            bytes: 0,
        }
    }

    pub fn append(&mut self, record: MemtableEntry) {
        let size = record.size_of();
        match self.buffer.insert(record.get_hash(), record) {
            Some(old) => {
                self.bytes += size - old.size_of();
            }
            None => {
                self.references += 1;
                self.bytes += size;
            }
        }
    }

    pub fn get(&self, hash: &HashedKey) -> &MemtableEntry {
        &self.buffer[hash]
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn references(&self) -> usize {
        self.references
    }

    pub fn iter(&self) -> std::collections::hash_map::Values<[u8; 20], MemtableEntry> {
        self.buffer.values()
    }

    pub fn truncate(&mut self) {
        self.buffer = HashMap::new();
    }
}
