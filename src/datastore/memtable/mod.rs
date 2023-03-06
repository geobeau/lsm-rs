use std::slice::Iter;

use crate::record::Record;

pub struct MemTable {
    buffer: Vec<Record>,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable { buffer: Vec::new() }
    }

    pub fn append(&mut self, record: Record) -> usize {
        self.buffer.push(record);
        self.buffer.len() - 1
    }

    pub fn get_offset(&self, offset: usize) -> &Record {
        &self.buffer[offset]
    }

    pub fn len(&self) -> usize {
        return self.buffer.len()
    }

    pub fn iter(&self) -> Iter<Record> {
        return self.buffer.iter();
    }
}
