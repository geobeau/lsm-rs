use crate::record::{HashedKey, Key, Record};

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
