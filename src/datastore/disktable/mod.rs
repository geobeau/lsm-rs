use std::{fs::{File, Metadata}, path::PathBuf, time::{UNIX_EPOCH, SystemTime}, io::{Seek, Write, Read}, rc::Rc, collections::HashMap};

use crate::record::Record;

use super::{memtable::{MemTable}, RecordMetadata};

// | metadata      |         data          |
// |num_of_elements|entry|entry|entry|entry|

// Represent an on-disk table
pub struct DiskTable {
    name: Rc<String>,
    count: usize, 
    fd: File,
}

impl DiskTable {
    pub fn new(name: Rc<String>, path: PathBuf) -> DiskTable {
        return DiskTable {
            name,
            count: 0,
            fd: File::options().create(true).read(true).write(true).open(path).unwrap(),
        }
    }

    pub fn flush_from_memtable(&mut self, memtable: &MemTable) -> Vec<RecordMetadata> {
        let mut offsets = Vec::new();
        self.fd.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        memtable.iter().for_each(
            |r| {
                offsets.push(RecordMetadata{
                    data_ptr: super::RecordPtr::DiskTable((self.name.clone(), buf.len())),
                    key_size: r.key.len(),
                    value_size: r.value.len(),
                    hash: r.hash.clone(),
                });
                self.count += 1;
                buf.extend(r.key.len().to_le_bytes());
                buf.extend(r.key.as_bytes());
                buf.extend(r.value.len().to_le_bytes());
                buf.extend(r.value.as_bytes());
            }
        );
        self.fd.write(&buf).unwrap();
        memtable.len();
        return offsets
    }
}

pub struct Manager {
    directory: PathBuf,
    tables: HashMap<Rc<String>, DiskTable>,
}

impl Manager {
    pub fn new(directory: PathBuf) -> Manager {
        Manager { directory, tables: HashMap::new() }
    }

    pub fn get(&mut self, meta: &RecordMetadata) -> Record {
        match &meta.data_ptr {
            super::RecordPtr::DiskTable((table_name, offset)) => {
                let table = self.tables.get_mut(table_name).unwrap();

                table.fd.seek(std::io::SeekFrom::Start(*offset as u64)).unwrap();
                let mut buf = vec![0u8;meta.key_size + meta.value_size + 16];
                table.fd.read_exact(&mut buf).unwrap();
                println!("Buffer: {:?}", buf);
                let key = std::str::from_utf8(&buf[8..8+meta.key_size]).unwrap();
                let value = std::str::from_utf8(&buf[16+meta.key_size..16+meta.key_size+meta.value_size]).unwrap();
                return Record::new(key.to_string(), value.to_string())
            },
            super::RecordPtr::MemTable(_) => panic!("Trying to query disk with a memtable pointer"),
        }
    }

    pub fn flush_memtable(&mut self, memtable: &MemTable) -> Vec<RecordMetadata> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let name = format!("{}-v1.data",timestamp.as_millis());
        let mut file_path = self.directory.clone();
        file_path.push(&name);
        let mut dt = DiskTable::new(Rc::new(name), file_path);
        let offsets = dt.flush_from_memtable(memtable);
        self.tables.insert(dt.name.clone(), dt);
        return offsets;
    }
}
