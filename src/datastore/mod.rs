use std::path::PathBuf;

use crate::record::{self, HashedKey, Record};

pub mod disktable;
pub mod index;
pub mod memtable;

#[derive(Debug, Clone)]
pub struct RecordMetadata {
    data_ptr: RecordPtr,
    offset: usize,
    key_size: usize,
    value_size: usize,
    hash: HashedKey,
}

#[derive(Debug, Clone)]
pub enum RecordPtr {
    DiskTable(usize),
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

    pub fn set(&mut self, record: Record) {
        let hash = record.hash;
        let key_size = record.key.len();
        let value_size = record.value.len();
        let id = self.memtable.append(record);

        let meta = RecordMetadata {
            data_ptr: RecordPtr::MemTable(()),
            offset: id,
            key_size,
            value_size,
            hash,
        };

        self.index.update(meta);
    }

    pub fn get(&self, key: &str) -> Option<&Record> {
        self.get_with_hash(record::hash_sha1(key))
    }

    pub fn get_with_hash(&self, hash: HashedKey) -> Option<&Record> {
        let meta = match self.index.get(hash) {
            Some(meta) => meta,
            None => return None,
        };
        match meta.data_ptr {
            RecordPtr::DiskTable(_) => todo!(),
            RecordPtr::MemTable(_) => Some(self.memtable.get_offset(meta.offset)),
        }
    }

    pub fn force_flush(&mut self) {
        self.table_manager.flush_memtable(&self.memtable);
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_datastore() {
        let mut storage = DataStore::new(PathBuf::from(r"./data/"));
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

        // let mut storage2 = Storage::new(false); // reload case
        // let opt = storage2.get("test1");
        // assert_eq!(opt.unwrap().as_ref(), "foo3");

        // let opt = storage2.get("test2");
        // assert_eq!(opt.unwrap().as_ref(), "foo2");
    }
}
