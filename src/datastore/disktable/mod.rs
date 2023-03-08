use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, Write},
    path::PathBuf,
    rc::Rc,
};

use crate::record::{hash_sha1, Record};

use super::{memtable::MemTable, RecordMetadata};

// | metadata      |         data          |
// |num_of_elements|entry|entry|entry|entry|

// |                         entry                          |
// |timestamp(u64le)|keysize(u16le)|valsize(u32le)|key|value|

// Represent an on-disk table
pub struct DiskTable {
    name: Rc<String>,
    path: PathBuf,
    count: u16,
    fd: File,
}

impl DiskTable {
    pub fn new(name: Rc<String>, path: PathBuf) -> DiskTable {
        return DiskTable {
            name,
            path: path.clone(),
            count: 0,
            fd: File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(path)
                .unwrap(),
        };
    }

    pub fn read_all_metadata(&mut self) -> Vec<RecordMetadata> {
        self.fd.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        self.fd.read_to_end(&mut buf).unwrap();
        let count = u16::from_le_bytes(buf[0..2].try_into().expect("incorrect length"));
        let mut meta = Vec::with_capacity(count as usize);
        let mut cursor = 2;
        for _ in 0..count {
            let key_size = u16::from_le_bytes(
                buf[cursor..cursor + 2]
                    .try_into()
                    .expect("incorrect length"),
            );
            let value_size = u32::from_le_bytes(
                buf[cursor + 2..cursor + 6]
                    .try_into()
                    .expect("incorrect length"),
            );
            let timestamp = u64::from_le_bytes(
                buf[cursor + 6..cursor + 14]
                    .try_into()
                    .expect("incorrect length"),
            );
            let key =
                std::str::from_utf8(&buf[cursor + 14..cursor + 14 + key_size as usize]).unwrap();

            meta.push(RecordMetadata {
                data_ptr: super::RecordPtr::DiskTable((self.name.clone(), cursor)),
                key_size,
                value_size,
                hash: hash_sha1(key),
                timestamp,
            });
            cursor += meta.last().unwrap().size_of();
        }
        meta
    }

    pub fn flush_from_memtable(&mut self, memtable: &MemTable) -> Vec<RecordMetadata> {
        let mut offsets = Vec::new();
        self.fd.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        buf.extend((memtable.len() as u16).to_le_bytes());
        memtable.iter().for_each(|e| {
            match e {
                super::MemtableEntry::Record(r) => {
                    offsets.push(RecordMetadata {
                        data_ptr: super::RecordPtr::DiskTable((self.name.clone(), buf.len())),
                        key_size: r.key.len() as u16,
                        value_size: r.value.len() as u32,
                        timestamp: r.timestamp,
                        hash: r.hash,
                    });
                    self.count += 1;
                    buf.extend((r.key.len() as u16).to_le_bytes());
                    buf.extend((r.value.len() as u32).to_le_bytes());
                    buf.extend(r.timestamp.to_le_bytes());
                    buf.extend(r.key.as_bytes());
                    buf.extend(r.value.as_bytes());
                }
                super::MemtableEntry::Tombstone(t) => {
                    offsets.push(RecordMetadata {
                        data_ptr: super::RecordPtr::DiskTable((self.name.clone(), buf.len())),
                        key_size: t.key.len() as u16,
                        value_size: 0,
                        timestamp: t.timestamp,
                        hash: t.hash,
                    });
                    self.count += 1;
                    buf.extend((t.key.len() as u16).to_le_bytes());
                    buf.extend((0u32).to_le_bytes());
                    buf.extend(t.timestamp.to_le_bytes());
                    buf.extend(t.key.as_bytes());
                }
            };
        });
        self.fd.write_all(&buf).unwrap();
        memtable.len();
        offsets
    }
}

pub struct Manager {
    directory: PathBuf,
    // TODO: make tables private and implement iterator
    pub tables: HashMap<Rc<String>, DiskTable>,
}

impl Manager {
    pub fn new(directory: PathBuf) -> Manager {
        Manager {
            directory,
            tables: HashMap::new(),
        }
    }

    pub fn init(&mut self) {
        let paths = std::fs::read_dir(&self.directory).unwrap();
        paths.for_each(|result| {
            let file = result.unwrap();
            let name = Rc::new(file.file_name().into_string().unwrap());
            let mut fd = File::options()
                .read(true)
                .write(true)
                .open(file.path())
                .unwrap();
            fd.seek(std::io::SeekFrom::Start(0)).unwrap();
            let mut count = [0u8; 2];
            fd.read_exact(&mut count).unwrap();
            self.tables.insert(
                name.clone(),
                DiskTable {
                    name,
                    path: file.path(),
                    count: u16::from_le_bytes(count),
                    fd,
                },
            );
        })
    }

    pub fn truncate(&mut self) {
        self.tables.drain().for_each(|(_, table)| {
            std::fs::remove_file(table.path).unwrap();
        })
    }

    pub fn get(&mut self, meta: &RecordMetadata) -> Record {
        match &meta.data_ptr {
            super::RecordPtr::DiskTable((table_name, offset)) => {
                let table = self.tables.get_mut(table_name).unwrap();

                table
                    .fd
                    .seek(std::io::SeekFrom::Start(*offset as u64))
                    .unwrap();
                let mut buf = vec![0u8; meta.size_of()];
                println!(
                    "position: {:?} meta: {:?}",
                    table.fd.stream_position().unwrap(),
                    meta
                );
                table.fd.read_exact(&mut buf).unwrap();
                let key = std::str::from_utf8(&buf[14..14 + meta.key_size as usize]).unwrap();
                let value = std::str::from_utf8(
                    &buf[14 + meta.key_size as usize
                        ..14 + meta.key_size as usize + meta.value_size as usize],
                )
                .unwrap();
                Record::new(key.to_string(), value.to_string())
            }
            _ => panic!("Trying to query disk with a non disk pointer"),
        }
    }

    pub fn flush_memtable(&mut self, memtable: &MemTable) -> Vec<RecordMetadata> {
        let name = format!("{}-v1.data", crate::time::now());
        let mut file_path = self.directory.clone();
        file_path.push(&name);
        let mut dt = DiskTable::new(Rc::new(name), file_path);
        let offsets = dt.flush_from_memtable(memtable);
        self.tables.insert(dt.name.clone(), dt);
        offsets
    }
}
