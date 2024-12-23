use std::{fs, path::PathBuf, rc::Rc};

use crate::record::{HashedKey, Key, Record};

use self::{disktable::ManagerStats, memtable::MemTable};

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
    DiskTable(DiskPointer),
    Compacting(HybridPointer),
    MemTable(MemtablePointer),
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiskPointer {
    disktable: Rc<String>,
    offset: u32,
}

// Not using composition here to have small structure
#[derive(Debug, Clone, PartialEq)]
pub struct HybridPointer {
    disktable: Rc<String>,
    d_offset: u32,
    memtable: u16,
    m_offset: u16,
}

impl HybridPointer {
    pub fn to_memtable_pointer(&self) -> MemtablePointer {
        MemtablePointer {
            memtable: self.memtable,
            offset: self.m_offset,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemtablePointer {
    memtable: u16,
    offset: u16,
}

pub struct DataStore {
    index: index::Index,
    memtable_manager: memtable::Manager,
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
            memtable_max_size_bytes: 4 * 1024 * 1024, // Should be much higher for a real db
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
    pub fn assert_not_corrupted(&self) {
        // println!("Stats: {:?}", self);
        assert_eq!(self.index_len, self.memtable_refs + self.disktable_refs);
        assert!(self.all_records >= self.index_len);
    }
}

impl DataStore {
    pub async fn new(directory: PathBuf) -> DataStore {
        DataStore::new_with_config(directory, Config::default()).await
    }

    pub async fn new_with_config(directory: PathBuf, config: Config) -> DataStore {
        fs::create_dir_all(directory.clone()).unwrap();
        DataStore {
            index: index::Index::new(),
            memtable_manager: memtable::Manager::new(config.memtable_max_size_bytes),
            table_manager: disktable::Manager::new(directory),
            config,
        }
    }

    pub async fn init(&mut self) {
        self.table_manager.init().await;
    }

    pub async fn truncate(&mut self) {
        self.index.truncate();
        self.memtable_manager.truncate();
        self.table_manager.truncate().await;
    }

    pub fn set(&self, record: Record) {
        self.set_raw(record);
    }

    pub fn delete(&self, key: &Key) {
        let timestamp = crate::time::now();
        self.set_raw(Record {
            key: key.clone(),
            value: vec![],
            timestamp,
        });
    }

    fn set_raw(&self, r: Record) {
        let hash = r.key.hash;
        let key_size = r.key.string.len() as u16;
        let value_size = r.value.len() as u32;
        let timestamp = r.timestamp;

        let ptr = match self.index.get(hash) {
            Some(m) => match m.data_ptr {
                RecordPtr::DiskTable(_) => self.memtable_manager.append(r),
                RecordPtr::Compacting(_) => self.memtable_manager.append(r),
                RecordPtr::MemTable(ptr) => self.memtable_manager.try_emplace(ptr, r),
            },
            None => self.memtable_manager.append(r),
        };

        let meta = RecordMetadata {
            data_ptr: RecordPtr::MemTable(ptr),
            key_size,
            value_size,
            timestamp,
            hash,
        };

        if let Some(old_meta) = self.index.update(meta) {
            self.remove_reference_from_storage(&old_meta);
        }
    }

    pub async fn get(&self, key: &Key) -> Option<Record> {
        let meta = match self.index.get(key.hash) {
            Some(meta) => meta,
            None => return None,
        };
        if meta.is_tombstone() {
            return None;
        }
        match meta.data_ptr {
            RecordPtr::DiskTable(_) => Some(self.table_manager.get(&meta).await),
            RecordPtr::MemTable(ptr) => Some(self.memtable_manager.get(&ptr)),
            RecordPtr::Compacting(ptr) => Some(self.memtable_manager.get(&ptr.to_memtable_pointer())),
        }
    }

    pub async fn rebuild_index_from_disk(&mut self) {
        let mut meta_to_update: Vec<RecordMetadata> = Vec::new();
        for t in self.table_manager.get_tables().into_iter() {
            let meta = t.read_all_metadata().await;
            let updates: Vec<RecordMetadata> = meta.into_iter().filter_map(|m| self.index.update(m)).collect();
            meta_to_update.extend(updates);
        }
        for meta in meta_to_update {
            self.remove_reference_from_storage(&meta);
        }
    }

    pub async fn clean_unused_disktables(&self) {
        self.table_manager.delete_disktables_marked_for_deletion();
    }

    pub async fn force_flush(&self) {
        for memtable in self.memtable_manager.get_all_unflushed_memtables() {
            self.flush_memtable(&memtable).await
        }
    }

    pub async fn flush_all_flushable_memtables(&self) {
        for memtable in self.memtable_manager.get_all_flushable_memtables() {
            self.flush_memtable(&memtable).await
        }
    }

    pub async fn flush_memtable(&self, memtable: &MemTable) {
        if memtable.is_empty() {
            return;
        }
        self.memtable_manager.mark_memtable_flushing(memtable.id);

        let offsets = self.table_manager.flush_memtable(memtable).await;
        let meta_to_update: Vec<RecordMetadata> = offsets
            .into_iter()
            // Update the index
            .filter_map(|m| self.index.update(m))
            .collect();
        for old_meta in meta_to_update {
            self.remove_reference_from_storage(&old_meta);
        }
        assert!(memtable.references() == 0);
        self.memtable_manager.truncate_memtable(memtable.id);
    }

    fn remove_reference_from_storage(&self, meta: &RecordMetadata) {
        match &meta.data_ptr {
            RecordPtr::DiskTable(ptr) => self.table_manager.remove_reference_from_storage(&ptr.disktable),
            RecordPtr::MemTable(ptr) => self.memtable_manager.remove_reference_from_memtable(ptr),
            RecordPtr::Compacting(ptr) => {
                self.table_manager.remove_reference_from_storage(&ptr.disktable);
                self.memtable_manager.remove_reference_from_memtable(&ptr.to_memtable_pointer());
            }
        };
    }

    async fn reclaim_disktable(&self, n: &Rc<String>) {
        let t = self.table_manager.get_table(n).unwrap();
        // TODO datastore should not access tables directly
        let mut to_remove = 0;
        let meta_to_update: Vec<RecordMetadata> = t
            .read_all_data()
            .await
            .into_iter()
            .filter_map(|(record, mut meta)| {
                if let Some(in_index_meta) = self.index.get(meta.hash) {
                    // Skip record if one is newer in memory
                    if meta.timestamp.lt(&in_index_meta.timestamp) {
                        to_remove += 1;
                        return Some(meta);
                    }
                }
                if meta.is_tombstone() && meta.timestamp < self.table_manager.get_oldest_table() {
                    self.index.delete(&meta);
                    return None;
                }
                let memtable_ptr = self.memtable_manager.append(record);
                if let RecordPtr::DiskTable(ptr) = meta.data_ptr {
                    meta.data_ptr = RecordPtr::Compacting(HybridPointer {
                        disktable: ptr.disktable,
                        d_offset: ptr.offset,
                        memtable: memtable_ptr.memtable,
                        m_offset: memtable_ptr.offset,
                    })
                }
                self.index.update(meta)
            })
            .collect();

        for meta in meta_to_update {
            self.remove_reference_from_storage(&meta);
        }
        t.set_as_pending_flush();
    }

    pub async fn maybe_run_one_reclaim(&self) {
        if let Some(n) = self.table_manager.get_best_table_to_reclaim() {
            println!("Reclaiming {}", n);
            self.reclaim_disktable(&n).await;
        }
    }

    pub async fn reclaim_all_disktables(&mut self) {
        for n in self.table_manager.list_tables() {
            self.reclaim_disktable(&n).await
        }
    }

    /// Return number of active records from memtable/index
    pub fn get_stats(&self) -> Stats {
        Stats {
            index_len: self.index.len(),
            memtable_refs: self.memtable_manager.references(),
            disktable_refs: self.table_manager.references(),
            disktable_manager_stats: self.table_manager.get_stats(),
            all_records: self.memtable_manager.len() + self.table_manager.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    fn assert_value_eq(r: &Record, expected: &str) {
        assert_eq!(std::str::from_utf8(&r.value).unwrap(), expected);
    }

    #[test]
    fn test_datastore_for_consistency() {
        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new().build().unwrap();

        rt.block_on(async {
            let mut storage = DataStore::new(PathBuf::from(r"./data/test/test_datastore_for_consistency")).await;
            storage.init().await;
            storage.truncate().await;
            let opt = storage.get(&Key::new("test".to_string())).await;
            assert!(opt.is_none());
            storage.get_stats().assert_not_corrupted();

            storage.set(Record::new("test1".to_string(), Vec::from("foo1".as_bytes())));
            let opt = storage.get(&Key::new("test1".to_string())).await;
            assert_value_eq(&opt.unwrap(), "foo1");
            storage.get_stats().assert_not_corrupted();

            storage.set(Record::new("test2".to_string(), Vec::from("foo2".as_bytes())));
            let opt = storage.get(&Key::new("test2".to_string())).await;
            assert_value_eq(&opt.unwrap(), "foo2");
            storage.get_stats().assert_not_corrupted();

            storage.set(Record::new("test3".to_string(), Vec::from("foo99".as_bytes())));
            let opt = storage.get(&Key::new("test3".to_string())).await;
            assert_value_eq(&opt.unwrap(), "foo99");
            storage.get_stats().assert_not_corrupted();

            storage.force_flush().await;
            storage.get_stats().assert_not_corrupted();

            storage.set(Record::new("test1".to_string(), Vec::from("foo3".as_bytes())));

            let opt = storage.get(&Key::new("test1".to_string())).await;
            assert_value_eq(&opt.unwrap(), "foo3");
            storage.get_stats().assert_not_corrupted();

            let opt = storage.get(&Key::new("test99999".to_string())).await; // unknown key
            assert!(opt.is_none());
            storage.get_stats().assert_not_corrupted();

            storage.delete(&Key::new("test3".to_string()));
            let opt = storage.get(&Key::new("test3".to_string())).await;
            assert!(opt.is_none());
            storage.get_stats().assert_not_corrupted();
            storage.force_flush().await;
            storage.get_stats().assert_not_corrupted();
            println!("{:?}", storage.get_stats());

            let opt = storage.get(&Key::new("test1".to_string())).await;
            assert_value_eq(&opt.unwrap(), "foo3");

            let mut storage2 = DataStore::new(PathBuf::from(r"./data/test/test_datastore_for_consistency")).await;
            storage2.init().await;
            storage2.get_stats().assert_not_corrupted();

            let opt = storage2.get(&Key::new("test1".to_string())).await;
            assert!(opt.is_none());
            storage2.get_stats().assert_not_corrupted();

            storage2.rebuild_index_from_disk().await;
            storage2.get_stats().assert_not_corrupted();

            let opt = storage2.get(&Key::new("test1".to_string())).await;
            assert_value_eq(&opt.unwrap(), "foo3");

            let opt = storage2.get(&Key::new("test2".to_string())).await;
            assert_value_eq(&opt.unwrap(), "foo2");
            storage2.get_stats().assert_not_corrupted();

            // Should have been deleted
            let opt = storage2.get(&Key::new("test3".to_string())).await;
            assert!(opt.is_none());
            storage2.get_stats().assert_not_corrupted();

            println!("{:?}", storage2.get_stats());
            storage2.reclaim_all_disktables().await;
            println!("{:?}", storage2.get_stats());
            assert_eq!(storage2.table_manager.get_disktables_marked_for_deletion().len(), 0);
            storage2.force_flush().await;
            assert_eq!(storage2.table_manager.get_disktables_marked_for_deletion().len(), 2);
            storage2.table_manager.delete_disktables_marked_for_deletion();
            storage2.get_stats().assert_not_corrupted();

            let opt = storage.get(&Key::new("test1".to_string())).await;
            assert_value_eq(&opt.unwrap(), "foo3");

            let opt = storage.get(&Key::new("test2".to_string())).await;
            assert_value_eq(&opt.unwrap(), "foo2");

            let opt = storage2.get(&Key::new("test3".to_string())).await;
            assert!(opt.is_none());

            println!("{:?}", storage.get_stats());
        });
    }

    #[test]
    fn test_datastore_for_flush_and_compactions() {
        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new().build().unwrap();

        rt.block_on(async {
            let mut storage = DataStore::new(PathBuf::from(r"./data/test/test_datastore_for_flush_and_compactions")).await;
            storage.init().await;
            storage.truncate().await;

            storage.set(Record::new("test1".to_string(), Vec::from("foo1".as_bytes())));
            storage.set(Record::new("test1".to_string(), Vec::from("foo1".as_bytes())));
            storage.set(Record::new("test1".to_string(), Vec::from("foo1".as_bytes())));
            storage.set(Record::new("test2".to_string(), Vec::from("foo2".as_bytes())));
            storage.set(Record::new("test3".to_string(), Vec::from("foo3".as_bytes())));
            storage.set(Record::new("test4".to_string(), Vec::from("foo4".as_bytes())));
            storage.set(Record::new("test5".to_string(), Vec::from("foo5".as_bytes())));
            storage.force_flush().await;

            storage.get_stats().assert_not_corrupted();

            storage.reclaim_all_disktables().await;
            storage.get_stats().assert_not_corrupted();

            // Try to flush empty memtable: should not add a new disktable
            storage.force_flush().await;
            storage.table_manager.delete_disktables_marked_for_deletion();
            assert_eq!(storage.get_stats().disktable_manager_stats.table_stats.len(), 1);
            storage.get_stats().assert_not_corrupted();

            // Reclaiming with only one table doesn't do anything
            storage.maybe_run_one_reclaim().await;
            assert_eq!(storage.get_stats().disktable_manager_stats.table_stats.len(), 1);
            storage.get_stats().assert_not_corrupted();

            storage.set(Record::new("test6".to_string(), Vec::from("foo6".as_bytes())));
            storage.set(Record::new("test7".to_string(), Vec::from("foo7".as_bytes())));
            storage.force_flush().await;
            assert_eq!(storage.get_stats().disktable_manager_stats.table_stats.len(), 2);
            storage.get_stats().assert_not_corrupted();

            println!("{:?}", storage.get_stats());
            // No reason to make a compaction
            storage.maybe_run_one_reclaim().await;
            assert_eq!(storage.get_stats().disktable_manager_stats.table_stats.len(), 2);
            storage.set(Record::new("test3".to_string(), Vec::from("foo31".as_bytes())));
            storage.set(Record::new("test4".to_string(), Vec::from("foo41".as_bytes())));

            storage.maybe_run_one_reclaim().await;
            storage.force_flush().await;
            storage.get_stats().assert_not_corrupted();
            assert_eq!(storage.table_manager.get_disktables_marked_for_deletion().len(), 1);
            storage.table_manager.delete_disktables_marked_for_deletion();
            assert_eq!(storage.get_stats().disktable_manager_stats.table_stats.len(), 2);

            // if we delete all data in a disktable, it should be ready for deletion
            storage.delete(&Key::new("test6".to_string()));
            storage.delete(&Key::new("test7".to_string()));
            storage.force_flush().await;
            assert_eq!(storage.table_manager.get_disktables_marked_for_deletion().len(), 1);
            storage.table_manager.delete_disktables_marked_for_deletion();
            storage.get_stats().assert_not_corrupted();

            storage.reclaim_all_disktables().await;
            storage.get_stats().assert_not_corrupted();
            storage.force_flush().await;
            storage.table_manager.delete_disktables_marked_for_deletion();
            assert_eq!(storage.table_manager.get_disktables_marked_for_deletion().len(), 0);
        });
    }
}
