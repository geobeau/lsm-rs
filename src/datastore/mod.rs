use std::{hash::Hash, path::PathBuf, rc::Rc};

use crate::record::{self, hash_sha1, HashedKey, Record};

use self::disktable::ManagerStats;

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
    DiskTable((Rc<String>, u32)),
    Compacting((Rc<String>, u32)),
    MemTable(()),
}

pub struct DataStore {
    index: index::Index,
    memtable: memtable::MemTable,
    table_manager: disktable::Manager,
    config: Config,
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

#[derive(Debug, Clone)]
pub struct Config {
    /// Number of bytes that can be stored in a given memtable before
    /// flushing to disktable
    pub memtable_max_size_bytes: usize,
    /// Ratio of in-use data in a disktable, going underneath will compact
    /// the table
    pub disktable_target_usage_ratio: f32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            memtable_max_size_bytes: 4096, // Should be much higher for a real db
            disktable_target_usage_ratio: 0.7,
        }
    }
}

#[derive(Debug)]
pub struct Stats {
    /// Number of records in the index
    /// Should be equal to memtable_refs and disktable_refs
    index_len: usize,
    /// Number of records in the memtable
    memtable_refs: usize,
    /// Number of records in the disktables
    disktable_refs: usize,
    /// Stats from the disktable manager
    disktable_manager_stats: ManagerStats,
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
        DataStore::new_with_config(directory, Config::default())
    }

    pub fn new_with_config(directory: PathBuf, config: Config) -> DataStore {
        if !directory.exists() {
            std::fs::create_dir_all(directory.clone()).unwrap();
        }
        DataStore {
            index: index::Index::new(),
            memtable: memtable::MemTable::new(),
            table_manager: disktable::Manager::new(directory),
            config,
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
        self.set_raw(record);
    }

    pub fn delete(&mut self, key: &str) {
        let hash = hash_sha1(key);
        let timestamp = crate::time::now();
        self.set_raw(Record {
            key: key.to_string(),
            value: "".to_string(),
            hash,
            timestamp,
        });
    }

    fn set_raw(&mut self, r: Record) {
        let hash = r.hash;
        let key_size = r.key.len() as u16;
        let value_size = r.value.len() as u32;
        let timestamp = r.timestamp;

        if !self.memtable.is_empty() && self.memtable.bytes + r.size_of() > self.config.memtable_max_size_bytes {
            self.force_flush()
        }

        self.memtable.append(r);

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

        meta_to_update.iter().for_each(|meta| self.remove_reference_from_storage(meta));
    }

    pub fn get_with_hash(&self, hash: HashedKey) -> Option<Record> {
        let meta = match self.index.get(hash) {
            Some(meta) => meta,
            None => return None,
        };
        if meta.is_tombstone() {
            return None;
        }
        match meta.data_ptr {
            RecordPtr::DiskTable(_) => Some(self.table_manager.get(meta)),
            RecordPtr::MemTable(_) => Some(self.memtable.get(&meta.hash).clone()),
            RecordPtr::Compacting(_) => Some(self.memtable.get(&meta.hash).clone()),
        }
    }

    pub fn force_flush(&mut self) {
        if self.memtable.is_empty() {
            return;
        }
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
            RecordPtr::DiskTable((table, _)) => self.table_manager.remove_reference_from_storage(table),
            RecordPtr::MemTable(_) => self.memtable.references -= 1,
            RecordPtr::Compacting((table, _)) => {
                self.table_manager.remove_reference_from_storage(table);
                self.memtable.references -= 1;
            }
        };
    }

    fn reclaim_disktable(&mut self, n: &Rc<String>) {
        let t = self.table_manager.tables.get_mut(n).unwrap();
        // TODO datastore should not access tables directly
        let meta_to_update: Vec<RecordMetadata> = t
            .read_all_data()
            .into_iter()
            .filter_map(|(record, mut meta)| {
                if let Some(in_index_meta) = self.index.get(meta.hash) {
                    // Skip record if one is newer in memory
                    if meta.timestamp.lt(&in_index_meta.timestamp) {
                        return Some(meta);
                    }
                }
                if meta.is_tombstone() && meta.timestamp < self.table_manager.oldest_table {
                    self.index.delete(&meta);
                    return None;
                }
                if let RecordPtr::DiskTable((t, o)) = meta.data_ptr {
                    meta.data_ptr = RecordPtr::Compacting((t, o))
                }
                self.memtable.append(record);
                self.index.update(meta)
            })
            .collect();

        meta_to_update.iter().for_each(|meta| self.remove_reference_from_storage(meta));
    }

    pub fn maybe_run_one_reclaim(&mut self) {
        if let Some(n) = self.table_manager.get_best_table_to_reclaim() {
            self.reclaim_disktable(&n);
        }
    }

    pub fn reclaim_all_disktables(&mut self) {
        let tables: Vec<Rc<String>> = self.table_manager.tables.iter_mut().map(|(n, _)| n).cloned().collect();

        tables.iter().for_each(|n| self.reclaim_disktable(n));
    }

    /// Return number of active records from memtable/index
    pub fn get_stats(&self) -> Stats {
        Stats {
            index_len: self.index.len(),
            memtable_refs: self.memtable.references(),
            disktable_refs: self.table_manager.references(),
            disktable_manager_stats: self.table_manager.get_stats(),
            all_records: self.memtable.len() + self.table_manager.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_datastore_for_consistency() {
        let mut storage = DataStore::new(PathBuf::from(r"./data/test/test_datastore_for_consistency"));
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
        println!("{:?}", storage.get_stats());

        let opt = storage.get("test1");
        assert_eq!(opt.unwrap().value, "foo3");

        let mut storage2 = DataStore::new(PathBuf::from(r"./data/test/test_datastore_for_consistency"));
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

        println!("{:?}", storage2.get_stats());
        storage2.reclaim_all_disktables();
        println!("{:?}", storage2.get_stats());
        assert_eq!(storage2.table_manager.get_disktables_marked_for_deletion().len(), 0);
        storage2.force_flush();
        assert_eq!(storage2.table_manager.get_disktables_marked_for_deletion().len(), 2);
        storage2.table_manager.delete_disktables_marked_for_deletion();
        storage2.get_stats().assert_not_corrupted();

        let opt = storage.get("test1");
        assert_eq!(opt.unwrap().value, "foo3");

        let opt = storage.get("test2");
        assert_eq!(opt.unwrap().value, "foo2");

        let opt = storage2.get("test3");
        assert!(opt.is_none());

        println!("{:?}", storage.get_stats());
    }

    #[test]
    fn test_datastore_for_flush_and_compactions() {
        let mut storage = DataStore::new(PathBuf::from(r"./data/test/test_datastore_for_flush_and_compactions"));
        storage.init();
        storage.truncate();

        storage.set(Record::new("test1".to_string(), "foo1".to_string()));
        storage.set(Record::new("test2".to_string(), "foo2".to_string()));
        storage.set(Record::new("test3".to_string(), "foo3".to_string()));
        storage.set(Record::new("test4".to_string(), "foo4".to_string()));
        storage.set(Record::new("test5".to_string(), "foo5".to_string()));
        storage.force_flush();

        storage.get_stats().assert_not_corrupted();

        // Try to flush empty memtable: should not add a new disktable
        storage.force_flush();
        assert_eq!(storage.get_stats().disktable_manager_stats.table_stats.len(), 1);

        // Reclaiming with only one table doesn't do anything
        storage.maybe_run_one_reclaim();
        assert_eq!(storage.get_stats().disktable_manager_stats.table_stats.len(), 1);

        storage.set(Record::new("test6".to_string(), "foo6".to_string()));
        storage.set(Record::new("test7".to_string(), "foo7".to_string()));
        storage.force_flush();
        assert_eq!(storage.get_stats().disktable_manager_stats.table_stats.len(), 2);

        println!("{:?}", storage.get_stats());
        // No reason to make a compaction
        storage.maybe_run_one_reclaim();
        assert_eq!(storage.get_stats().disktable_manager_stats.table_stats.len(), 2);
        storage.set(Record::new("test3".to_string(), "foo31".to_string()));
        storage.set(Record::new("test4".to_string(), "foo41".to_string()));

        storage.maybe_run_one_reclaim();
        storage.force_flush();
        assert_eq!(storage.table_manager.get_disktables_marked_for_deletion().len(), 1);
        storage.table_manager.delete_disktables_marked_for_deletion();
        assert_eq!(storage.get_stats().disktable_manager_stats.table_stats.len(), 2);

        // if we delete all data in a disktable, it should be ready for deletion
        storage.delete("test6");
        storage.delete("test7");
        storage.force_flush();
        assert_eq!(storage.table_manager.get_disktables_marked_for_deletion().len(), 1);
        storage.table_manager.delete_disktables_marked_for_deletion();

        storage.reclaim_all_disktables();
        storage.force_flush();
        storage.table_manager.delete_disktables_marked_for_deletion();
        assert_eq!(storage.table_manager.get_disktables_marked_for_deletion().len(), 1);
    }
}
