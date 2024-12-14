use crate::{
    record::{HashedKey, Key, Record},
    topology::{self, ReactorMetadata, Topology},
};

#[derive(Debug)]
pub enum Command {
    Data(DataCommand),
    Cluster(ClusterCommand),
}

#[derive(Debug)]
pub enum DataCommand {
    Get(Get),
    Delete(Delete),
    Set(Set),
}

#[derive(Debug)]
pub enum ClusterCommand {
    Join(Join),
}

#[derive(Debug)]
pub struct Join {
    pub reactors: Vec<ReactorMetadata>,
}

impl DataCommand {
    pub fn get_hash(&self) -> &HashedKey {
        match self {
            DataCommand::Get(c) => &c.key.hash,
            DataCommand::Delete(c) => &c.key.hash,
            DataCommand::Set(c) => &c.record.key.hash,
        }
    }

    /// get the shard number between 0 and 16384 (`cluster::MAX_RANGE`) using crc16
    pub fn get_slot(&self) -> u16 {
        self.get_crc16() % topology::MAX_RANGE
    }

    // TODO: maybe pre-calculate it?
    pub fn get_crc16(&self) -> u16 {
        let key = match self {
            DataCommand::Get(c) => &c.key.string,
            DataCommand::Delete(c) => &c.key.string,
            DataCommand::Set(c) => &c.record.key.string,
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
    ClusterTopology(ClusterTopologyResp),
}

pub struct GetResp {
    pub record: Option<Record>,
}

pub struct SetResp {}

pub struct DeleteResp {}

pub struct ClusterTopologyResp {
    pub topology: Topology,
}
