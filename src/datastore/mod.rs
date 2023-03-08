use std::{hash::Hash, path::PathBuf, rc::Rc};

use crate::record::{self, hash_sha1, HashedKey, Record};

pub mod disktable;
pub mod index;
pub mod memtable;

#[derive(Debug, Clone)]
pub struct RecordMetadata {
    key_size: u16,
    value_size: u32,
    timestamp: u64,
    hash: HashedKey,
    data_ptr: RecordPtr,
}

impl RecordMetadata {
    /// Return the size in number of bytes of the record
    pub fn size_of(&self) -> usize {
        self.key_size as usize + self.value_size as usize + 14
    }

    pub fn is_tombstone(&self) -> bool {
        self.value_size == 0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RecordPtr {
    DiskTable((Rc<String>, usize)),
    MemTable(()),
}

pub struct DataStore {
    index: index::Index,
    memtable: memtable::MemTable,
    table_manager: disktable::Manager,
}

#[derive(Debug, Clone)]
pub enum MemtableEntry {
    Record(Record),
    Tombstone(Tombstone),
}

impl MemtableEntry {
    pub fn size_of(&self) -> usize {
        match self {
            MemtableEntry::Record(r) => r.size_of(),
            MemtableEntry::Tombstone(t) => t.size_of(),
        }
    }

    pub fn get_hash(&self) -> HashedKey {
        match self {
            MemtableEntry::Record(r) => r.hash,
            MemtableEntry::Tombstone(t) => t.hash,
        }
    }

    pub fn get_key_len(&self) -> usize {
        match self {
            MemtableEntry::Record(r) => r.key.len(),
            MemtableEntry::Tombstone(t) => t.key.len(),
        }
    }

    pub fn get_timestamp(&self) -> u64 {
        match self {
            MemtableEntry::Record(r) => r.timestamp,
            MemtableEntry::Tombstone(t) => t.timestamp,
        }
    }

    pub fn get_value_len(&self) -> usize {
        match self {
            MemtableEntry::Record(r) => r.value.len(),
            MemtableEntry::Tombstone(_) => 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Tombstone {
    pub key: String,
    pub hash: HashedKey,
    pub timestamp: u64,
}

impl Tombstone {
    pub fn size_of(&self) -> usize {
        2 + 4 + 8 + self.key.len()
    }
}

pub struct Stats {
    /// Number of records in the index
    /// Should be equal to memtable_refs and disktable_refs
    index_len: usize,
    /// Number of records in the memtable
    memtable_refs: usize,
    /// Number of records in the disktables
    disktable_refs: usize,
    /// Total number of records inside the table
    /// Should be >= index_refs
    all_records: usize,
}

impl Stats {
    fn assert_not_corrupted(&self) {
        assert_eq!(self.index_len, self.memtable_refs + self.disktable_refs);
        assert!(self.all_records >= self.index_len);
    }
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
        self.index.truncate();
        self.memtable.truncate();
        self.table_manager.truncate();
    }

    pub fn set(&mut self, record: Record) {
        self.set_raw(MemtableEntry::Record(record));
    }

    pub fn delete(&mut self, key: &str) {
        let hash = hash_sha1(key);
        let timestamp = crate::time::now();
        self.set_raw(MemtableEntry::Tombstone(Tombstone {
            key: key.to_string(),
            hash,
            timestamp,
        }));
    }

    fn set_raw(&mut self, e: MemtableEntry) {
        let hash = e.get_hash();
        let key_size = e.get_key_len() as u16;
        let value_size = e.get_value_len() as u32;
        let timestamp = e.get_timestamp();
        self.memtable.append(e);

        let meta = RecordMetadata {
            data_ptr: RecordPtr::MemTable(()),
            key_size,
            value_size,
            timestamp,
            hash,
        };

        if let Some(old_meta) = self.index.update(meta) {
            self.remove_reference_from_storage(&old_meta);
        }
    }

    pub fn get(&mut self, key: &str) -> Option<Record> {
        self.get_with_hash(record::hash_sha1(key))
    }

    pub fn rebuild_index_from_disk(&mut self) {
        let meta_to_update: Vec<RecordMetadata> = self
            .table_manager
            .tables
            .iter_mut()
            .flat_map(|(_, t)| t.read_all_metadata())
            .filter_map(|meta| self.index.update(meta))
            .collect();

        meta_to_update
            .iter()
            .for_each(|meta| self.remove_reference_from_storage(meta));
    }

    pub fn get_with_hash(&mut self, hash: HashedKey) -> Option<Record> {
        let meta = match self.index.get(hash) {
            Some(meta) => meta,
            None => return None,
        };
        if meta.is_tombstone() {
            return None;
        }
        match meta.data_ptr {
            RecordPtr::DiskTable(_) => Some(self.table_manager.get(meta)),
            RecordPtr::MemTable(_) => {
                match self.memtable.get(&meta.hash) {
                    MemtableEntry::Record(r) => Some(r.clone()),
                    // TODO: remove/log as it should not be reached (discard at result)
                    MemtableEntry::Tombstone(_) => None,
                }
            }
        }
    }

    pub fn force_flush(&mut self) {
        let offsets = self.table_manager.flush_memtable(&self.memtable);
        let meta_to_update: Vec<RecordMetadata> = offsets
            .into_iter()
            // Update the index
            .filter_map(|m| self.index.update(m))
            .collect();

        meta_to_update
            .iter()
            // Make sure the references are correctly handled
            .for_each(|old_meta| self.remove_reference_from_storage(old_meta));
        assert!(self.memtable.references == 0);
        self.memtable.truncate();
    }

    fn remove_reference_from_storage(&mut self, meta: &RecordMetadata) {
        match &meta.data_ptr {
            RecordPtr::DiskTable((table, _)) => {
                self.table_manager.remove_reference_from_storage(table)
            }
            RecordPtr::MemTable(_) => self.memtable.references -= 1,
        };
    }

    /// Return number of active records from memtable/index
    pub fn get_stats(&self) -> Stats {
        Stats {
            index_len: self.index.len(),
            memtable_refs: self.memtable.references(),
            disktable_refs: self.table_manager.references(),
            all_records: self.memtable.len() + self.table_manager.len(),
        }
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
        storage.get_stats().assert_not_corrupted();

        storage.set(Record::new("test1".to_string(), "foo1".to_string()));
        let opt = storage.get("test1");
        assert_eq!(opt.unwrap().value, "foo1");
        storage.get_stats().assert_not_corrupted();

        storage.set(Record::new("test2".to_string(), "foo2".to_string()));
        let opt = storage.get("test2");
        assert_eq!(opt.unwrap().value, "foo2");
        storage.get_stats().assert_not_corrupted();

        storage.set(Record::new("test3".to_string(), "foo99".to_string()));
        let opt = storage.get("test3");
        assert_eq!(opt.unwrap().value, "foo99");
        storage.get_stats().assert_not_corrupted();

        storage.force_flush();
        storage.get_stats().assert_not_corrupted();

        storage.set(Record::new("test1".to_string(), "foo3".to_string()));
        let opt = storage.get("test1");
        assert_eq!(opt.unwrap().value, "foo3");
        storage.get_stats().assert_not_corrupted();

        let opt = storage.get("test99999"); // unknown key
        assert!(opt.is_none());
        storage.get_stats().assert_not_corrupted();

        storage.delete("test3");
        let opt = storage.get("test3");
        assert!(opt.is_none());
        storage.get_stats().assert_not_corrupted();
        storage.force_flush();
        storage.get_stats().assert_not_corrupted();

        let opt = storage.get("test1");
        assert_eq!(opt.unwrap().value, "foo3");

        let mut storage2 = DataStore::new(PathBuf::from(r"./data/"));
        storage2.init();
        storage2.get_stats().assert_not_corrupted();

        let opt = storage2.get("test1");
        assert!(opt.is_none());
        storage2.get_stats().assert_not_corrupted();

        storage2.rebuild_index_from_disk();
        storage2.get_stats().assert_not_corrupted();

        let opt = storage2.get("test1");
        assert_eq!(opt.unwrap().value, "foo3");

        let opt = storage2.get("test2");
        assert_eq!(opt.unwrap().value, "foo2");
        storage2.get_stats().assert_not_corrupted();

        // Should have been deleted
        let opt = storage2.get("test3");
        assert!(opt.is_none());
        storage2.get_stats().assert_not_corrupted();
    }
}
