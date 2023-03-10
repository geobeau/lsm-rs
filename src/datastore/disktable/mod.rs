use futures::prelude::*;
use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::io::{ImmutableFile, ImmutableFileBuilder};
use std::{collections::HashMap, path::PathBuf, rc::Rc};

use crate::record::{hash_sha1_bytes, Record};

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
    /// Count the number of records physically within the disktables
    count: u16,
    /// Count the number of references to disktable from the index
    /// 0 means that the table is safe for deletion
    references: u16,
    /// Mark the disktable for deletion
    deletion_marker: bool,
    /// File descriptor of the table on disk
    fd: ImmutableFile,
}

#[derive(Debug)]
pub struct DiskTableStats {
    pub usage_ratio: f32,
}

impl DiskTable {
    pub async fn new_from_memtable(name: Rc<String>, path: PathBuf, timestamp: u64, memtable: &MemTable) -> (DiskTable, Vec<RecordMetadata>) {
        let mut sink = ImmutableFileBuilder::new(path.clone())
            .with_pre_allocation(Some(memtable.bytes as u64))
            .build_sink()
            .await
            .unwrap();

        let mut offsets = Vec::with_capacity(memtable.len());
        let mut buf: Vec<u8> = Vec::with_capacity(memtable.bytes);
        let mut count = 0;
        let mut references = 0;

        buf.extend((memtable.len() as u16).to_le_bytes());
        buf.extend(crate::time::now().to_le_bytes());
        memtable.iter().for_each(|r| {
            offsets.push(RecordMetadata {
                data_ptr: super::RecordPtr::DiskTable((name.clone(), buf.len() as u32)),
                key_size: r.key.len() as u16,
                value_size: r.value.len() as u32,
                timestamp: r.timestamp,
                hash: r.hash,
            });
            count += 1;
            references += 1;
            buf.extend((r.key.len() as u16).to_le_bytes());
            buf.extend((r.value.len() as u32).to_le_bytes());
            buf.extend(r.timestamp.to_le_bytes());
            buf.extend(r.key.as_bytes());
            buf.extend(r.value.as_bytes());
        });
        sink.write_all(&buf);
        memtable.len();

        (
            DiskTable {
                name,
                path,
                timestamp,
                count: 0,
                references: 0,
                deletion_marker: false,
                fd: sink.seal().await.unwrap(),
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
            count: u16::from_le_bytes(buf[0..2].try_into().unwrap()),
            timestamp,
            references: 0,
            deletion_marker: false,
            fd,
        }
    }

    pub async fn read_all_metadata(&mut self) -> Vec<RecordMetadata> {
        let mut stream = self.fd.stream_reader().build();
        let mut header_buffer = [0u8, 10];
        stream.read_exact(&mut header_buffer).await.unwrap();
        let count = u16::from_le_bytes(header_buffer[0..2].try_into().unwrap());

        let mut meta = Vec::with_capacity(count as usize);

        let mut cursor = 10;
        let mut record_metadata_buffer = [0u8, 14];
        for _ in 0..count {
            stream.read_exact(&mut record_metadata_buffer).await.unwrap();
            let key_size = u16::from_le_bytes(record_metadata_buffer[0..2].try_into().expect("incorrect length"));
            let value_size = u32::from_le_bytes(record_metadata_buffer[2..6].try_into().expect("incorrect length"));
            let timestamp = u64::from_le_bytes(record_metadata_buffer[6..14].try_into().expect("incorrect length"));

            let mut key = vec![0u8; key_size as usize];
            stream.read_exact(&mut key);
            stream.skip(value_size as u64);

            meta.push(RecordMetadata {
                data_ptr: super::RecordPtr::DiskTable((self.name.clone(), cursor as u32)),
                key_size,
                value_size,
                hash: hash_sha1_bytes(&key),
                timestamp,
            });
            self.references += 1;
            cursor += meta.last().unwrap().size_of();
        }
        meta
    }

    pub async fn read_all_data(&mut self) -> Vec<(Record, RecordMetadata)> {
        let mut stream = self.fd.stream_reader().build();
        let mut header_buffer = [0u8, 10];
        stream.read_exact(&mut header_buffer).await.unwrap();
        let count = u16::from_le_bytes(header_buffer[0..2].try_into().unwrap());
        let mut record_metadata_buffer = [0u8, 14];

        let mut meta = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let offset = stream.current_pos();
            stream.read_exact(&mut record_metadata_buffer).await.unwrap();
            let key_size = u16::from_le_bytes(record_metadata_buffer[0..2].try_into().expect("incorrect length"));
            let value_size = u32::from_le_bytes(record_metadata_buffer[2..6].try_into().expect("incorrect length"));
            let timestamp = u64::from_le_bytes(record_metadata_buffer[6..14].try_into().expect("incorrect length"));

            let mut key = vec![0u8; key_size as usize];
            stream.read_exact(&mut key);
            let value = vec![0u8; value_size as usize];
            stream.read_exact(&mut key);

            let hash = hash_sha1_bytes(&key);

            meta.push((
                Record {
                    hash,
                    timestamp,
                    key: std::str::from_utf8(&key).unwrap().to_string(),
                    value: std::str::from_utf8(&value).unwrap().to_string(),
                },
                RecordMetadata {
                    data_ptr: super::RecordPtr::DiskTable((self.name.clone(), offset as u32)),
                    key_size,
                    value_size,
                    hash,
                    timestamp,
                },
            ));
            self.references += 1;
        }
        meta
    }

    fn decr_reference(&mut self) {
        self.references -= 1;
        if self.references == 0 {
            self.deletion_marker = true
        }
    }

    async fn get(&self, meta: &RecordMetadata, offset: u32) -> Record {
        let result = self.fd.read_at(offset as u64, meta.size_of()).await.unwrap();
        let timestamp = u64::from_le_bytes(result[6..14].try_into().expect("incorrect length"));
        let key = std::str::from_utf8(&result[14..14 + meta.key_size as usize]).unwrap();
        let value = std::str::from_utf8(&result[14 + meta.key_size as usize..14 + meta.key_size as usize + meta.value_size as usize]).unwrap();

        Record::new_with_timestamp(key.to_string(), value.to_string(), timestamp)
    }

    pub fn get_usage_ratio(&self) -> f32 {
        self.references as f32 / self.count as f32
    }

    pub fn get_stats(&self) -> DiskTableStats {
        DiskTableStats {
            usage_ratio: self.get_usage_ratio(),
        }
    }
}

pub struct Manager {
    directory: PathBuf,
    // TODO: make tables private and implement iterator
    pub tables: HashMap<Rc<String>, DiskTable>,
    pub oldest_table: u64,
}

#[derive(Debug)]
pub struct ManagerStats {
    pub table_stats: Vec<(Rc<String>, DiskTableStats)>,
}

impl Manager {
    pub fn new(directory: PathBuf) -> Manager {
        Manager {
            oldest_table: crate::time::now(),
            directory,
            tables: HashMap::new(),
        }
    }

    fn refresh_oldest_table(&mut self) {
        self.oldest_table = self.tables.values().map(|t| t.timestamp).min().unwrap_or_else(crate::time::now)
    }

    pub async fn init(&mut self) {
        let paths = std::fs::read_dir(&self.directory).unwrap();
        for result in paths {
            let file = result.unwrap();
            let name = Rc::new(file.file_name().into_string().unwrap());
            self.tables.insert(name.clone(), DiskTable::new_from_disk(name, file.path()).await);
        }

        self.refresh_oldest_table();
    }

    pub fn truncate(&mut self) {
        self.tables.drain().for_each(|(_, table)| {
            std::fs::remove_file(table.path).unwrap();
        })
    }

    pub async fn get(&self, meta: &RecordMetadata) -> Record {
        match &meta.data_ptr {
            super::RecordPtr::DiskTable((table_name, offset)) => {
                self.tables.get(table_name).unwrap().get(meta, *offset).await
            }
            _ => panic!("Trying to query disk with a non disk pointer"),
        }
    }

    pub async fn flush_memtable(&mut self, memtable: &MemTable) -> Vec<RecordMetadata> {
        let now = crate::time::now();
        let name = format!("{}-v1.data", now);
        let mut file_path = self.directory.clone();
        file_path.push(&name);
        let (dt, offsets) = DiskTable::new_from_memtable(Rc::from(name), file_path, now, memtable).await;
        self.tables.insert(dt.name.clone(), dt);
        self.refresh_oldest_table();
        offsets
    }

    pub fn remove_reference_from_storage(&mut self, table: &Rc<String>) {
        self.tables.get_mut(table).unwrap().decr_reference()
    }

    pub fn get_disktables_marked_for_deletion(&self) -> Vec<Rc<String>> {
        self.tables.iter().filter(|(_, t)| t.deletion_marker).map(|(n, _)| n).cloned().collect()
    }

    pub fn delete_disktables_marked_for_deletion(&mut self) {
        self.get_disktables_marked_for_deletion().iter().for_each(|t| {
            let table = self.tables.get(t).unwrap();
            std::fs::remove_file(&table.path).unwrap();
            self.tables.remove(t);
        });
    }

    pub fn references(&self) -> usize {
        self.tables.iter().fold(0, |size, t| size + t.1.references as usize)
    }

    pub fn len(&self) -> usize {
        self.tables.iter().fold(0, |size, t| size + t.1.count as usize)
    }

    pub fn get_stats(&self) -> ManagerStats {
        ManagerStats {
            table_stats: self.tables.iter().map(|(n, t)| (n.clone(), t.get_stats())).collect(),
        }
    }

    pub fn list_tables(&self) -> Vec<Rc<String>> {
        self.tables.keys().cloned().collect()
    }

    pub fn get_best_table_to_reclaim(&self) -> Option<Rc<String>> {
        // TODO: Make ratio configurable
        let target_ratio = 0.7;
        self.tables
            .iter()
            .find(|(_n, t)| t.get_usage_ratio() < target_ratio)
            .map(|(n, _)| n.clone())
    }
}
