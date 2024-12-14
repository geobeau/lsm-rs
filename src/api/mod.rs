use crate::{
    record::{HashedKey, Key, Record},
    topology,
};

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Delete(Delete),
    Set(Set),
}

impl Command {
    pub fn get_hash(&self) -> &HashedKey {
        match self {
            Command::Get(c) => &c.key.hash,
            Command::Delete(c) => &c.key.hash,
            Command::Set(c) => &c.record.key.hash,
        }
    }

    /// get the shard number between 0 and 16384 (`cluster::MAX_RANGE`) using crc16
    pub fn get_slot(&self) -> u16 {
        self.get_crc16() % topology::MAX_RANGE
    }

    // TODO: maybe pre-calculate it?
    pub fn get_crc16(&self) -> u16 {
        let key = match self {
            Command::Get(c) => &c.key.string,
            Command::Delete(c) => &c.key.string,
            Command::Set(c) => &c.record.key.string,
        };
        return crc16_xmodem_fast::hash(key.as_bytes()) as u16;
    }
}

#[derive(Debug)]
pub struct Get {
    pub key: Key,
}

#[derive(Debug)]
pub struct Delete {
    pub key: Key,
}

#[derive(Debug)]
pub struct Set {
    pub record: Record,
}

pub enum Response {
    Get(GetResp),
    Delete(DeleteResp),
    Set(SetResp),
}

pub struct GetResp {
    pub record: Option<Record>,
}

pub struct SetResp {}

pub struct DeleteResp {}
