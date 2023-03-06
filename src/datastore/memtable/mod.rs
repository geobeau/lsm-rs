use crate::record::Record;

pub struct MemTable {
    buffer: Vec<Record>
}

impl MemTable {
    pub fn new() -> MemTable {
        return MemTable { buffer: Vec::new() }
    }

    pub fn append(&mut self, record: Record) -> usize {
        self.buffer.push(record);
        return self.buffer.len() - 1
    }

    pub fn get_offset(&self, offset: usize) -> &Record {
        return &self.buffer[offset]
    }
}