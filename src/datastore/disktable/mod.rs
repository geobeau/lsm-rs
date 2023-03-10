use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    io::{Read, Seek, Write},
    path::PathBuf,
    rc::Rc,
};

use crate::record::{hash_sha1, Record};

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
    fd: RefCell<File>,
}

#[derive(Debug)]
pub struct DiskTableStats {
    pub usage_ratio: f32,
}

impl DiskTable {
    pub fn new(name: Rc<String>, path: PathBuf, timestamp: u64) -> DiskTable {
        let fd = File::options().create(true).read(true).write(true).open(path.clone()).unwrap();
        DiskTable {
            name,
            path,
            timestamp,
            count: 0,
            references: 0,
            deletion_marker: false,
            fd: RefCell::from(fd),
        }
    }

    /// Initialize a disktable from an already existing table
    pub fn new_from_disk(name: Rc<String>, path: PathBuf) -> DiskTable {
        // Open the file and read its disktable metadata
        let mut fd = File::options().read(true).write(true).open(path.clone()).unwrap();
        fd.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut count = [0u8; 2];
        fd.read_exact(&mut count).unwrap();
        let mut timestamp_raw = [0u8; 8];
        fd.read_exact(&mut timestamp_raw).unwrap();
        let timestamp = u64::from_le_bytes(timestamp_raw);
        crate::time::sync(timestamp);

        DiskTable {
            name,
            path,
            count: u16::from_le_bytes(count),
            timestamp,
            references: 0,
            deletion_marker: false,
            fd: RefCell::from(fd),
        }
    }

    pub fn read_all_metadata(&mut self) -> Vec<RecordMetadata> {
        self.fd.borrow_mut().seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        self.fd.borrow_mut().read_to_end(&mut buf).unwrap();
        let count = u16::from_le_bytes(buf[0..2].try_into().expect("incorrect length"));
        let mut meta = Vec::with_capacity(count as usize);
        let mut cursor = 10;
        for _ in 0..count {
            let key_size = u16::from_le_bytes(buf[cursor..cursor + 2].try_into().expect("incorrect length"));
            let value_size = u32::from_le_bytes(buf[cursor + 2..cursor + 6].try_into().expect("incorrect length"));
            let timestamp = u64::from_le_bytes(buf[cursor + 6..cursor + 14].try_into().expect("incorrect length"));
            let key = std::str::from_utf8(&buf[cursor + 14..cursor + 14 + key_size as usize]).unwrap();

            meta.push(RecordMetadata {
                data_ptr: super::RecordPtr::DiskTable((self.name.clone(), cursor as u32)),
                key_size,
                value_size,
                hash: hash_sha1(key),
                timestamp,
            });
            self.references += 1;
            cursor += meta.last().unwrap().size_of();
        }
        meta
    }

    pub fn read_all_data(&mut self) -> Vec<(Record, RecordMetadata)> {
        self.fd.borrow_mut().seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        self.fd.borrow_mut().read_to_end(&mut buf).unwrap();
        let count = u16::from_le_bytes(buf[0..2].try_into().expect("incorrect length"));
        let _ = u64::from_le_bytes(buf[2..10].try_into().expect("incorrect length"));
        let mut meta = Vec::with_capacity(count as usize);
        let mut cursor = 10;
        for _ in 0..count {
            let key_size = u16::from_le_bytes(buf[cursor..cursor + 2].try_into().expect("incorrect length"));
            let value_size = u32::from_le_bytes(buf[cursor + 2..cursor + 6].try_into().expect("incorrect length"));
            let timestamp = u64::from_le_bytes(buf[cursor + 6..cursor + 14].try_into().expect("incorrect length"));
            let key = std::str::from_utf8(&buf[cursor + 14..cursor + 14 + key_size as usize]).unwrap();
            let value = std::str::from_utf8(&buf[cursor + 14 + key_size as usize..cursor + 14 + key_size as usize + value_size as usize]).unwrap();

            let hash = hash_sha1(key);
            meta.push((
                Record {
                    hash,
                    timestamp,
                    key: key.to_string(),
                    value: value.to_string(),
                },
                RecordMetadata {
                    data_ptr: super::RecordPtr::DiskTable((self.name.clone(), cursor as u32)),
                    key_size,
                    value_size,
                    hash,
                    timestamp,
                },
            ));
            self.references += 1;
            cursor += meta.last().unwrap().1.size_of();
        }
        meta
    }

    pub fn flush_from_memtable(&mut self, memtable: &MemTable) -> Vec<RecordMetadata> {
        let mut offsets = Vec::new();
        self.fd.borrow_mut().seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        buf.extend((memtable.len() as u16).to_le_bytes());
        buf.extend(crate::time::now().to_le_bytes());
        memtable.iter().for_each(|r| {
            offsets.push(RecordMetadata {
                data_ptr: super::RecordPtr::DiskTable((self.name.clone(), buf.len() as u32)),
                key_size: r.key.len() as u16,
                value_size: r.value.len() as u32,
                timestamp: r.timestamp,
                hash: r.hash,
            });
            self.count += 1;
            self.references += 1;
            buf.extend((r.key.len() as u16).to_le_bytes());
            buf.extend((r.value.len() as u32).to_le_bytes());
            buf.extend(r.timestamp.to_le_bytes());
            buf.extend(r.key.as_bytes());
            buf.extend(r.value.as_bytes());
        });
        self.fd.borrow_mut().write_all(&buf).unwrap();
        memtable.len();
        offsets
    }

    fn decr_reference(&mut self) {
        self.references -= 1;
        if self.references == 0 {
            self.deletion_marker = true
        }
    }

    fn get(&self, meta: &RecordMetadata, offset: u32) -> Record {
        self.fd.borrow_mut().seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
        let mut buf = vec![0u8; meta.size_of()];
        self.fd.borrow_mut().read_exact(&mut buf).unwrap();
        let timestamp = u64::from_le_bytes(buf[6..14].try_into().expect("incorrect length"));
        let key = std::str::from_utf8(&buf[14..14 + meta.key_size as usize]).unwrap();
        let value = std::str::from_utf8(&buf[14 + meta.key_size as usize..14 + meta.key_size as usize + meta.value_size as usize]).unwrap();

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

    pub fn init(&mut self) {
        let paths = std::fs::read_dir(&self.directory).unwrap();
        paths.for_each(|result| {
            let file = result.unwrap();
            let name = Rc::new(file.file_name().into_string().unwrap());

            self.tables.insert(name.clone(), DiskTable::new_from_disk(name, file.path()));
        });
        self.refresh_oldest_table();
    }

    pub fn truncate(&mut self) {
        self.tables.drain().for_each(|(_, table)| {
            std::fs::remove_file(table.path).unwrap();
        })
    }

    pub fn get(&self, meta: &RecordMetadata) -> Record {
        match &meta.data_ptr {
            super::RecordPtr::DiskTable((table_name, offset)) => {
                return self.tables.get(table_name).unwrap().get(meta, *offset);
            }
            _ => panic!("Trying to query disk with a non disk pointer"),
        }
    }

    pub fn flush_memtable(&mut self, memtable: &MemTable) -> Vec<RecordMetadata> {
        let now = crate::time::now();
        let name = format!("{}-v1.data", now);
        let mut file_path = self.directory.clone();
        file_path.push(&name);
        let mut dt = DiskTable::new(Rc::new(name), file_path, now);
        let offsets = dt.flush_from_memtable(memtable);
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

    pub fn get_best_table_to_reclaim(&self) -> Option<Rc<String>> {
        // TODO: Make ratio configurable
        let target_ratio = 0.7;
        self.tables
            .iter()
            .find(|(_n, t)| t.get_usage_ratio() < target_ratio)
            .map(|(n, _)| n.clone())
    }
}
