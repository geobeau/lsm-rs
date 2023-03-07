use std::{path::PathBuf, rc::Rc};

use crate::record::{self, HashedKey, Record};

pub mod disktable;
pub mod index;
pub mod memtable;

#[derive(Debug, Clone)]
pub struct RecordMetadata {
    data_ptr: RecordPtr,
    key_size: u16,
    value_size: u32,
    timestamp: u64,
    hash: HashedKey,
}

impl RecordMetadata {
    /// Return the size in number of bytes of the record
    pub fn size_of(&self) -> usize {
        return self.key_size as usize + self.value_size as usize + 14
    }
}

#[derive(Debug, Clone)]
pub enum RecordPtr {
    DiskTable((Rc<String>, usize)),
    MemTable(()),
}

pub struct DataStore {
    index: index::Index,
    memtable: memtable::MemTable,
    table_manager: disktable::Manager,
}

impl DataStore {
    pub fn new(directory: PathBuf) -> DataStore {
        DataStore {
            index: index::Index::new(),
            memtable: memtable::MemTable::new(),
            table_manager: disktable::Manager::new(directory),
        }
    }

    pub fn init(&mut self) {
        self.table_manager.init();
    }

    pub fn truncate(&mut self) {
        self.memtable.truncate();
        self.table_manager.truncate();
    }

    pub fn set(&mut self, record: Record) {
        let hash = record.hash;
        let key_size = record.key.len() as u16;
        let value_size = record.value.len() as u32;
        let timestamp = record.timestamp;
        self.memtable.append(record);

        let meta = RecordMetadata {
            data_ptr: RecordPtr::MemTable(()),
            key_size,
            value_size,
            timestamp,
            hash,
        };

        self.index.update(meta);
    }

    pub fn get(&mut self, key: &str) -> Option<Record> {
        self.get_with_hash(record::hash_sha1(key))
    }

    pub fn get_with_hash(&mut self, hash: HashedKey) -> Option<Record> {
        let meta = match self.index.get(hash) {
            Some(meta) => meta,
            None => return None,
        };
        match meta.data_ptr {
            RecordPtr::DiskTable(_) => Some(self.table_manager.get(&meta)),
            RecordPtr::MemTable(_) => Some(self.memtable.get(&meta.hash).clone()),
        }
    }

    pub fn force_flush(&mut self) {
        let offsets = self.table_manager.flush_memtable(&self.memtable);
        offsets.into_iter()
        // Update the index
        .filter_map(|m| self.index.update(m))
        // Make sure the references are correctly handled
        .for_each(|old_meta| {
            match old_meta.data_ptr {
                RecordPtr::DiskTable(_) => panic!("Unexpected record on disk it should have been in memory, old meta: {:?}", old_meta),
                RecordPtr::MemTable(_) => self.memtable.references -= 1,
            };
        });
        assert!(self.memtable.references == 0);
        self.memtable.truncate();
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_datastore() {
        let mut storage = DataStore::new(PathBuf::from(r"./data/"));
        storage.init();
        storage.truncate();
        let opt = storage.get("test");
        assert!(opt.is_none());

        storage.set(Record::new("test1".to_string(), "foo1".to_string()));
        let opt = storage.get("test1");
        assert_eq!(opt.unwrap().value, "foo1");

        storage.set(Record::new("test2".to_string(), "foo2".to_string()));
        let opt = storage.get("test2");
        assert_eq!(opt.unwrap().value, "foo2");

        storage.set(Record::new("test1".to_string(), "foo3".to_string()));
        let opt = storage.get("test1");
        assert_eq!(opt.unwrap().value, "foo3");

        let opt = storage.get("test99999"); // unknown key
        assert!(opt.is_none());

        storage.force_flush();
        let opt = storage.get("test1");
        assert_eq!(opt.unwrap().value, "foo3");

        // let mut storage2 = Storage::new(false); // reload case
        // let opt = storage2.get("test1");
        // assert_eq!(opt.unwrap().as_ref(), "foo3");

        // let opt = storage2.get("test2");
        // assert_eq!(opt.unwrap().as_ref(), "foo2");
    }
}
