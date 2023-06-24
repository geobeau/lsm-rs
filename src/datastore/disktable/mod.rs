use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::io::{ImmutableFile, ImmutableFileBuilder};
use glommio::sync::RwLock;
use std::cell::{Cell, RefCell};
use std::{collections::HashMap, path::PathBuf, rc::Rc};

use crate::record::{hash_sha1_bytes, Key, Record};

use super::DiskPointer;
use super::{memtable::MemTable, RecordMetadata};

/// Represent an on-disk table
///
/// | metadata      |         data          |
/// |num_of_elements|entry|entry|entry|entry|
///
/// |                         entry                          |
/// |timestamp(u64le)|keysize(u16le)|valsize(u32le)|key|value|
pub struct DiskTable {
    name: Rc<String>,
    path: PathBuf,
    timestamp: u64,
    fd: RwLock<ImmutableFile>,
    /// Count the number of records physically within the disktables
    count: Cell<u16>,
    /// Count the number of references to disktable from the index
    /// 0 means that the table is safe for deletion
    references: Cell<u16>,
    /// Mark the disktable for deletion
    deletion_marker: Cell<bool>,
}

#[derive(Debug)]
pub struct DiskTableStats {
    pub usage_ratio: f32,
    pub references: usize,
    pub count: usize,
}

impl DiskTable {
    pub async fn new_from_memtable(name: Rc<String>, path: PathBuf, timestamp: u64, memtable: &MemTable) -> (DiskTable, Vec<RecordMetadata>) {
        let mut sink = ImmutableFileBuilder::new(path.clone())
            .with_pre_allocation(Some(memtable.get_byte_size() as u64))
            .build_sink()
            .await
            .unwrap();

        let mut offsets = Vec::with_capacity(memtable.len());
        let mut buf: Vec<u8> = Vec::with_capacity(memtable.get_byte_size());
        let mut count = 0;
        let mut references = 0;

        buf.extend((memtable.len() as u16).to_le_bytes());
        buf.extend(crate::time::now().to_le_bytes());
        memtable.values().iter().for_each(|r| {
            offsets.push(RecordMetadata {
                data_ptr: super::RecordPtr::DiskTable(DiskPointer { disktable: name.clone(), offset: buf.len() as u32 }),
                key_size: r.key.string.len() as u16,
                value_size: r.value.len() as u32,
                timestamp: r.timestamp,
                hash: r.key.hash,
            });
            buf.extend((r.key.string.len() as u16).to_le_bytes());
            buf.extend((r.value.len() as u32).to_le_bytes());
            buf.extend(r.timestamp.to_le_bytes());
            buf.extend(r.key.string.as_bytes());
            buf.extend(r.value.clone());
            count += 1;
            references += 1;
        });
        sink.write_all(&buf).await.unwrap();
        memtable.len();

        (
            DiskTable {
                name,
                path,
                timestamp,
                fd: RwLock::new(sink.seal().await.unwrap()),
                count: Cell::new(count),
                references: Cell::new(references),
                deletion_marker: Cell::new(false),
            },
            offsets,
        )
    }

    /// Initialize a disktable from an already existing table
    pub async fn new_from_disk(name: Rc<String>, path: PathBuf) -> DiskTable {
        // Open the file and read its disktable metadata
        let fd = ImmutableFileBuilder::new(path.clone()).build_existing().await.unwrap();
        let buf = fd.read_at(0, 10).await.unwrap();
        let timestamp = u64::from_le_bytes(buf[2..10].try_into().unwrap());
        crate::time::sync(timestamp);

        DiskTable {
            name,
            path,
            timestamp,
            fd: RwLock::new(fd),
            count: Cell::new(u16::from_le_bytes(buf[0..2].try_into().unwrap())),
            references: Cell::new(0),
            deletion_marker: Cell::new(false),
        }
    }

    pub async fn read_all_metadata(&self) -> Vec<RecordMetadata> {
        let fd = self.fd.write().await.unwrap();
        let mut stream = fd.stream_reader().build();
        let mut header_buffer = [0u8; 10];
        stream.read_exact(&mut header_buffer).await.unwrap();
        let count = u16::from_le_bytes(header_buffer[0..2].try_into().unwrap());

        let mut meta = Vec::with_capacity(count as usize);
        let mut cursor: usize = 10;
        let mut record_metadata_buffer = [0u8; 14];
        for _ in 0..count {
            stream.read_exact(&mut record_metadata_buffer).await.unwrap();
            let key_size = u16::from_le_bytes(record_metadata_buffer[0..2].try_into().expect("incorrect length"));
            let value_size = u32::from_le_bytes(record_metadata_buffer[2..6].try_into().expect("incorrect length"));
            let timestamp = u64::from_le_bytes(record_metadata_buffer[6..14].try_into().expect("incorrect length"));
            let mut key = vec![0u8; key_size as usize];
            stream.read_exact(&mut key).await.unwrap();
            stream.skip(value_size as u64);

            meta.push(RecordMetadata {
                data_ptr: super::RecordPtr::DiskTable(DiskPointer { disktable: self.name.clone(), offset: cursor as u32 }),
                key_size,
                value_size,
                hash: hash_sha1_bytes(&key),
                timestamp,
            });
            self.references.set(self.references.get() + 1);
            cursor += meta.last().unwrap().size_of();
        }
        meta
    }

    pub async fn read_all_data(&self) -> Vec<(Record, RecordMetadata)> {
        let fd = self.fd.write().await.unwrap();
        let mut stream = fd.stream_reader().build();
        let mut header_buffer = [0u8; 10];
        stream.read_exact(&mut header_buffer).await.unwrap();
        let count = u16::from_le_bytes(header_buffer[0..2].try_into().unwrap());
        let mut record_metadata_buffer = [0u8; 14];

        let mut meta = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let offset = stream.current_pos();
            stream.read_exact(&mut record_metadata_buffer).await.unwrap();
            let key_size = u16::from_le_bytes(record_metadata_buffer[0..2].try_into().expect("incorrect length"));
            let value_size = u32::from_le_bytes(record_metadata_buffer[2..6].try_into().expect("incorrect length"));
            let timestamp = u64::from_le_bytes(record_metadata_buffer[6..14].try_into().expect("incorrect length"));

            let mut key_bytes = vec![0u8; key_size as usize];
            stream.read_exact(&mut key_bytes).await.unwrap();
            let mut value = vec![0u8; value_size as usize];
            stream.read_exact(&mut value).await.unwrap();

            let key = Key::new(std::str::from_utf8(&key_bytes).unwrap().to_string());
            let hash = key.hash;
            meta.push((
                Record { timestamp, key, value },
                RecordMetadata {
                    data_ptr: super::RecordPtr::DiskTable(DiskPointer { disktable: self.name.clone(), offset: offset as u32 }),
                    key_size,
                    value_size,
                    hash,
                    timestamp,
                },
            ));
            self.references.set(self.references.get() + 1);
        }
        meta
    }

    async fn decr_reference(&self) {
        self.references.set(self.references.get() - 1);
        if self.references.get() == 0 {
            self.deletion_marker.set(true)
        }
    }

    async fn get(&self, meta: &RecordMetadata, offset: u32) -> Record {
        let fd = self.fd.write().await.unwrap();
        let result = fd.read_at(offset as u64, meta.size_of()).await.unwrap();
        let timestamp = u64::from_le_bytes(result[6..14].try_into().expect("incorrect length"));
        let key = std::str::from_utf8(&result[14..14 + meta.key_size as usize]).unwrap();
        let value = Vec::from(&result[14 + meta.key_size as usize..14 + meta.key_size as usize + meta.value_size as usize]);

        Record::new_with_timestamp(key.to_string(), value, timestamp)
    }

    pub fn get_stats(&self) -> DiskTableStats {
        DiskTableStats {
            usage_ratio: self.references.get() as f32 / self.count.get() as f32,
            references: self.references.get() as usize,
            count: self.count.get() as usize,
        }
    }

    pub fn is_marked_for_deletion(&self) -> bool {
        self.deletion_marker.get()
    }
}

pub struct Manager {
    directory: PathBuf,
    tables: RefCell<HashMap<Rc<String>, Rc<DiskTable>>>,
    oldest_table: Cell<u64>,
}

#[derive(Debug)]
pub struct ManagerStats {
    pub table_stats: Vec<(Rc<String>, DiskTableStats)>,
}

impl Manager {
    pub fn new(directory: PathBuf) -> Manager {
        Manager {
            oldest_table: Cell::from(crate::time::now()),
            directory,
            tables: RefCell::from(HashMap::new()),
        }
    }

    fn refresh_oldest_table(&self) {
        self.oldest_table
            .set(self.tables.borrow().values().map(|t| t.timestamp).min().unwrap_or_else(crate::time::now))
    }

    pub async fn init(&self) {
        let paths = std::fs::read_dir(&self.directory).unwrap();
        for result in paths {
            let file = result.unwrap();
            let name = Rc::new(file.file_name().into_string().unwrap());
            let dt = Rc::from(DiskTable::new_from_disk(name.clone(), file.path()).await);
            self.tables.borrow_mut().insert(name, dt);
        }

        self.refresh_oldest_table();
    }

    pub async fn truncate(&mut self) {
        for (_, table) in self.tables.borrow_mut().drain() {
            // write() is used here because the table is going to be destroyed
            // ensure only one ref is in use (ours)
            assert_eq!(Rc::strong_count(&table), 1);
            table.fd.close().unwrap();
            std::fs::remove_file(table.path.clone()).unwrap();
        }
    }

    pub async fn get(&self, meta: &RecordMetadata) -> Record {
        match &meta.data_ptr {
            super::RecordPtr::DiskTable(ptr) => self.tables.borrow().get(&ptr.disktable).unwrap().get(meta, ptr.offset).await,
            _ => panic!("Trying to query disk with a non disk pointer"),
        }
    }

    pub async fn flush_memtable(&self, memtable: &MemTable) -> Vec<RecordMetadata> {
        let now = crate::time::now();
        let name = format!("{}-v1.data", now);
        let mut file_path = self.directory.clone();
        file_path.push(&name);
        let (dt, offsets) = DiskTable::new_from_memtable(Rc::from(name), file_path, now, memtable).await;
        self.tables.borrow_mut().insert(dt.name.clone(), Rc::from(dt));
        self.refresh_oldest_table();
        offsets
    }

    pub async fn remove_reference_from_storage(&self, table: &Rc<String>) {
        self.tables.borrow_mut().get_mut(table).unwrap().decr_reference().await
    }

    pub fn get_disktables_marked_for_deletion(&self) -> Vec<Rc<String>> {
        self.tables
            .borrow()
            .iter()
            .filter(|(_, t)| t.is_marked_for_deletion())
            .map(|(n, _)| n)
            .cloned()
            .collect()
    }

    pub fn delete_disktables_marked_for_deletion(&mut self) {
        let table_marked_deletion = self.get_disktables_marked_for_deletion();
        let mut tables_mut = self.tables.borrow_mut();
        table_marked_deletion.iter().for_each(|t| {
            let table = tables_mut.get(t).unwrap();
            std::fs::remove_file(&table.path).unwrap();
            tables_mut.remove(t);
        });
    }

    pub fn references(&self) -> usize {
        self.tables.borrow().values().fold(0, |size, t| size + t.get_stats().references)
    }

    pub fn len(&self) -> usize {
        self.tables.borrow().values().fold(0, |size, t| size + t.get_stats().count)
    }

    pub fn get_stats(&self) -> ManagerStats {
        ManagerStats {
            table_stats: self.tables.borrow().iter().map(|(n, t)| (n.clone(), t.get_stats())).collect(),
        }
    }

    pub fn list_tables(&self) -> Vec<Rc<String>> {
        self.tables.borrow().keys().cloned().collect()
    }

    pub fn get_table(&self, name: &Rc<String>) -> Option<Rc<DiskTable>> {
        self.tables.borrow().get(name).cloned()
    }

    pub fn get_tables(&self) -> Vec<Rc<DiskTable>> {
        self.tables.borrow().values().cloned().collect()
    }

    pub fn get_oldest_table(&self) -> u64 {
        self.oldest_table.get()
    }

    pub async fn get_best_table_to_reclaim(&self) -> Option<Rc<String>> {
        // TODO: Make ratio configurable
        let target_ratio = 0.7;
        self.tables
            .borrow()
            .iter()
            .find(|(_n, t)| t.get_stats().usage_ratio < target_ratio)
            .map(|(n, _)| n.clone())
    }
}
