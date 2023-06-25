use std::{cell::{RefCell, Cell}, borrow::BorrowMut, rc::Rc};

use crate::{record::{Record}};

use super::MemtablePointer;

pub struct MemTable {
    pub id: u16,
    buffer: RefCell<Vec<Record>>,
    stats: RefCell<Stats>,
    status: Cell<MemtableStatus>
}


#[derive(Debug, Copy, Clone)]
pub enum MemtableStatus {
    Open,
    Flushable,
    Flushing,
}

struct Stats {
    /// Number of references on it from the index
    pub references: usize,
    /// Number of bytes of data added to it
    pub bytes: usize,
}

impl MemTable {
    pub fn new(id: u16) -> MemTable {
        MemTable {
            id,
            buffer: RefCell::from(Vec::with_capacity(usize::pow(2, 16))),
            stats: RefCell::from(Stats { references: 0, bytes: 0 }),
            status: Cell::from(MemtableStatus::Open)
        }
    }

    pub fn append(&self, record: Record) -> u16 {
        let size = record.size_of();
        let mut mutable_stats = self.stats.borrow_mut();
        let mut mutable_buffer = self.buffer.borrow_mut();
        let offset = mutable_buffer.len();
        mutable_buffer.push(record);
        mutable_stats.references += 1;
        mutable_stats.bytes += size;
        return offset as u16
    }

    pub fn get(&self, ptr: &MemtablePointer) -> Record {
        self.buffer.borrow()[ptr.offset as usize].clone()
    }

    pub fn len(&self) -> usize {
        self.buffer.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.borrow().is_empty()
    }

    pub fn is_unflushed(&self) -> bool {
        match self.status.get() {
            MemtableStatus::Open => true,
            MemtableStatus::Flushable => true,
            MemtableStatus::Flushing => false,
        }
    }

    pub fn should_be_flushed(&self) -> bool {
        match self.status.get() {
            MemtableStatus::Open => false,
            MemtableStatus::Flushable => true,
            MemtableStatus::Flushing => false,
        }
    }

    pub fn mark_flushing(&self) {
        self.status.set(MemtableStatus::Flushing)
    }

    pub fn references(&self) -> usize {
        self.stats.borrow().references
    }

    pub fn decr_references(&self, val: usize) {
        self.stats.borrow_mut().references -= val;
    }

    pub fn get_byte_size(&self) -> usize {
        self.stats.borrow().bytes
    }

    pub fn values(&self) -> Vec<Record> {
        self.buffer.borrow().clone()
    }

    pub fn truncate(&self) {
        let mut mutable_stats = self.stats.borrow_mut();
        self.buffer.borrow_mut().clear();

        mutable_stats.bytes = 0;
        mutable_stats.references = 0;
        self.status.set(MemtableStatus::Open);
    }
}


pub struct Manager {
    tables: RefCell<MemtableList>,
    memtable_max_size_bytes: usize,
    cur_memtable: Cell<u16>
}

impl Manager {
    pub fn new(memtable_max_size_bytes: usize) -> Manager {
        let mut tables = MemtableList::new();
        let id = tables.get_next_free();
        
        Manager {
            tables: RefCell::from(tables),
            cur_memtable: Cell::from(id),
            memtable_max_size_bytes,
        }
    }

    pub fn append(&self, record: Record) -> MemtablePointer {
        let mut tables = self.tables.borrow_mut();
        let mut memtable = tables.get(self.cur_memtable.get());
        if memtable.get_byte_size() + record.size_of() > self.memtable_max_size_bytes {
            println!("Marking as flushable: {}, {}", memtable.get_byte_size(), memtable.id);
            memtable.status.set(MemtableStatus::Flushable);
            let id = tables.get_next_free();
            self.cur_memtable.set(id);
            memtable = tables.get(id);
        }
        let offset = memtable.borrow_mut().append(record);

        return MemtablePointer{memtable: self.cur_memtable.get(), offset: offset as u16}
    }

    pub fn get(&self, ptr: &MemtablePointer) -> Record {
        let tables = self.tables.borrow();
        tables.get(ptr.memtable).get(ptr).clone()
    }

    pub fn remove_reference_from_memtable(&self, ptr: &MemtablePointer) {
        let tables = self.tables.borrow();
        tables.get(ptr.memtable).decr_references(1)
    }

    pub fn truncate(&self) {
        let mut tables = self.tables.borrow_mut();
        tables.truncate();
        self.cur_memtable.set(tables.get_next_free());
    }

    pub fn truncate_memtable(&self, id: u16) {
        println!("truncating: {id}");
        self.tables.borrow_mut().delete(id)
    }

    pub fn mark_memtable_flushing(&self, id: u16) {
        if self.cur_memtable.get() == id {
            self.cur_memtable.set(self.tables.borrow_mut().get_next_free());
        }
        self.tables.borrow().get(id).mark_flushing();
    }

    pub fn len(&self) -> usize {
        self.tables.borrow().iter()
        .filter(|e| e.next_free.is_none())
        .fold(0, |total, entry| total + entry.table.len())
    }

    pub fn references(&self) -> usize {
        self.tables.borrow().iter()
        .filter(|e| e.next_free.is_none())
        .fold(0, |total, entry| total + entry.table.references())
    }

    pub fn get_all_unflushed_memtables(&self) -> Vec<Rc<MemTable>> {
        self.tables.borrow_mut().iter()
            .filter(|e| e.next_free.is_none())
            .map(|e| e.table.clone())
            .filter(|t| t.is_unflushed())
            .collect()
    }

    pub fn get_all_flushable_memtables(&self) -> Vec<Rc<MemTable>> {
        self.tables.borrow_mut().iter()
            .filter(|e| e.next_free.is_none())
            .map(|e| e.table.clone())
            .filter(|t| t.should_be_flushed())
            .collect()
    }
}


pub struct Entry {
    next_free: Option<u16>, 
    table: Rc<MemTable> 
}


struct MemtableList {
    list: Vec<Entry>,
    last_free: usize,
}

impl MemtableList {
    pub fn new() -> MemtableList {
        MemtableList {
            list: Vec::new(),
            last_free: 0,
        }
    }

    pub fn get(&self, offset: u16) -> &MemTable {
        if self.list[offset as usize].next_free.is_some() {
            panic!("trying to reach truncated memtable")
        }
        return &self.list[offset as usize].table
    }

    pub fn get_next_free(&mut self) -> u16 {
        if self.last_free >= self.list.len() {
            let offset = self.list.len();
            if self.list.len() > u16::MAX as usize {
                panic!("Too much data: {} (max={})", self.list.len(), u16::MAX)
            }
            self.list.push(Entry{table: Rc::from(MemTable::new(offset as u16)), next_free: None });
            self.last_free = self.list.len();
            return offset as u16;
        }

        let offset = match &self.list[self.last_free].next_free {
            Some(p) => {
                let offset = self.last_free;
                self.last_free = *p as usize;
                offset
            },
            None => panic!("offset: {} should be free but contains data", self.last_free),
        };
        self.list[offset].next_free = None; 
        return offset as u16;
    }

    pub fn delete(&mut self, offset: u16) {
        if self.list[offset as usize].next_free.is_none() {
            self.list[offset as usize].table.truncate();
            self.list[offset as usize].next_free = Some(self.last_free as u16);
            self.last_free = offset as usize;
        }
    }

    pub fn truncate(&mut self) {
        self.last_free = self.list.len();
        for i in 0..self.list.len() {
            self.list[i].table.truncate();
            self.list[i].next_free= Some(self.last_free as u16);
            self.last_free = i;
        }
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Entry>{
        self.list.iter()
    }
}