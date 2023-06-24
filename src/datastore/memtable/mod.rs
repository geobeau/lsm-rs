use std::{cell::RefCell, collections::HashMap};

use crate::record::{HashedKey, Record};

use super::MemtablePointer;

pub struct MemTable {
    buffer: RefCell<Vec<Record>>,
    stats: RefCell<Stats>,
}

struct Stats {
    /// Number of references on it from the index
    pub references: usize,
    /// Number of bytes of data added to it
    pub bytes: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            buffer: RefCell::from(Vec::with_capacity(usize::pow(2, 16))),
            stats: RefCell::from(Stats { references: 0, bytes: 0 }),
        }
    }

    pub fn append(&self, record: Record) -> MemtablePointer {
        let size = record.size_of();
        let mut mutable_stats = self.stats.borrow_mut();
        let mut mutable_buffer = self.buffer.borrow_mut();
        let offset = mutable_buffer.len();
        mutable_buffer.push(record);
        mutable_stats.references += 1;
        mutable_stats.bytes += size;
        return MemtablePointer{memtable: 0, offset: offset as u16}
    }

    pub fn get(&self, ptr: &MemtablePointer) -> Record {
        self.buffer.borrow()[ptr.offset as usize].clone()
    }

    pub fn len(&self) -> usize {
        self.buffer.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.borrow().is_empty()
    }

    pub fn references(&self) -> usize {
        self.stats.borrow().references
    }

    pub fn decr_references(&self, val: usize) {
        self.stats.borrow_mut().references -= val;
    }

    pub fn get_byte_size(&self) -> usize {
        self.stats.borrow().bytes
    }

    pub fn values(&self) -> Vec<Record> {
        self.buffer.borrow().clone()
    }

    pub fn truncate(&self) {
        let mut mutable_stats = self.stats.borrow_mut();
        self.buffer.borrow_mut().clear();

        mutable_stats.bytes = 0;
        mutable_stats.references = 0;
    }
}
