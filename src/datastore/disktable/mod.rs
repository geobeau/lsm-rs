use std::fs::File;

// | metadata      |         data          |
// |num_of_elements|entry|entry|entry|entry|

// Represent an on-disk table
pub struct DiskTable {
    name: String,
    fd: File
}

impl DiskTable {
    pub fn new() -> DiskTable {
        return DiskTable { name: todo!(), fd: todo!() };
    }
}

pub struct Manager {
    tables: Vec<DiskTable>
}

impl Manager {
    pub fn new() -> Manager {
        return Manager { tables: Vec::new() };
    }
}