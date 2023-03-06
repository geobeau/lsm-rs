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
}
