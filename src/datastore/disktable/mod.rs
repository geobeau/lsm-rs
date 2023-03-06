use std::{fs::File, path::PathBuf, time::{UNIX_EPOCH, SystemTime}, io::{Seek, Write}};

use super::memtable::{MemTable};

// | metadata      |         data          |
// |num_of_elements|entry|entry|entry|entry|

// Represent an on-disk table
pub struct DiskTable {
    name: String,
    fd: File,
}

impl DiskTable {
    pub fn new(name: String, path: PathBuf) -> DiskTable {
        return DiskTable {
            name,
            fd: File::options().create(true).read(true).write(true).open(path).unwrap(),
        }
    }

    pub fn flush_from_memtable(&mut self, memtable: &MemTable) -> Vec<usize> {
        let mut offsets = Vec::new();
        self.fd.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        memtable.iter().for_each(
            |r| {
                offsets.push(buf.len());
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
    tables: Vec<DiskTable>,
}

impl Manager {
    pub fn new(directory: PathBuf) -> Manager {
        Manager { directory, tables: Vec::new() }
    }

    pub fn flush_memtable(&mut self, memtable: &MemTable) {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let name = format!("{}-v1.data",timestamp.as_millis());
        let mut file_path = self.directory.clone();
        file_path.push(&name);
        let mut dt = DiskTable::new(name, file_path);
        dt.flush_from_memtable(memtable);
        self.tables.push(dt);
    }
}
