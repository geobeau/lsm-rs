mod shard;

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    path::PathBuf,
    rc::Rc,
};

use shard::Shard;

use crate::{
    api::{Command, DeleteResp, GetResp, Response, SetResp},
    cluster::ClusterMessage,
    topology::{self, ReactorMetadata, Topology},
};

#[derive(Debug)]
pub struct CommandHandle {
    pub command: Command,
    // pub sender: SharedSender<Response>,
}

/// Provide safe access to shards
struct Shards {
    shards: RefCell<HashMap<u16, Rc<Shard>>>,
}

// Since it is using refcell, no await should be used while borrowing `shards`
impl Shards {
    pub fn new() -> Shards {
        Shards {
            shards: RefCell::new(HashMap::new()),
        }
    }

    pub fn get_shard(&self, shard_id: &u16) -> Option<Rc<Shard>> {
        self.shards.borrow().get(shard_id).cloned()
    }

    pub fn insert_shard(&self, shard_id: u16, shard: Rc<Shard>) {
        self.shards.borrow_mut().insert(shard_id, shard);
    }

    pub fn remove_shard(&self, shard_id: &u16) -> Option<Rc<Shard>> {
        self.shards.borrow_mut().remove(shard_id)
    }

    pub fn keys(&self) -> Vec<u16> {
        return self.shards.borrow().keys().cloned().collect();
    }

    pub fn len(&self) -> usize {
        return self.shards.borrow().len();
    }
}

pub struct StorageProxy {
    shards: Shards,
    pub shards_count: u16,
    data_dir: PathBuf,
    reactor_metadata: ReactorMetadata,
    topology: RefCell<Option<Rc<Topology>>>,
    cluster_sender: async_channel::Sender<ClusterMessage>,
}

impl StorageProxy {
    pub fn new(
        reactor_metadata: ReactorMetadata,
        shards_count: u16,
        cluster_sender: async_channel::Sender<ClusterMessage>,
        data_dir: &PathBuf,
    ) -> StorageProxy {
        StorageProxy {
            reactor_metadata,
            shards: Shards::new(),
            shards_count,
            data_dir: data_dir.clone(),
            topology: RefCell::from(None),
            cluster_sender,
        }
    }

    pub async fn apply_new_topology(&self, topology: &Topology) {
        let shard_ranges = topology.reactor_allocations.get(&self.reactor_metadata).unwrap();

        let mut incoming_shards = HashSet::with_capacity(shard_ranges.len());
        shard_ranges.iter().for_each(|sr| {
            incoming_shards.insert(sr.start);
        });

        let mut existing_shards = HashSet::with_capacity(self.shards.len());
        self.shards.keys().into_iter().for_each(|s| {
            existing_shards.insert(s);
        });

        let shards_to_add = incoming_shards.difference(&existing_shards);
        let shards_to_remove = existing_shards.difference(&incoming_shards);

        for start in shards_to_add {
            let mut shard_path = PathBuf::new();
            shard_path.push(format!("{}", start));
            let shard = Shard::new(self.reactor_metadata.id, self.data_dir.join(shard_path)).await;
            self.shards.insert_shard(*start, shard);
        }

        for start in shards_to_remove {
            match self.shards.remove_shard(start) {
                Some(_) => todo!(),
                None => todo!(),
            }
        }

        let _ = self.topology.borrow_mut().insert(Rc::from(topology.clone()));
    }

    pub async fn dispatch_local(&self, shard: Rc<Shard>, cmd: Command) -> Response {
        match cmd {
            Command::Get(c) => {
                let record = shard.datastore.get(&c.key).await;
                Response::Get(GetResp { record })
            }
            Command::Delete(c) => {
                shard.datastore.delete(&c.key);
                Response::Delete(DeleteResp {})
            }
            Command::Set(c) => {
                shard.datastore.set(c.record);
                Response::Set(SetResp {})
            }
        }
    }

    pub async fn dispatch(&self, cmd: Command) -> Response {
        let cmd_slot = cmd.get_slot();
        let shard_id = topology::compute_shard_id(cmd_slot, self.shards_count);
        // println!("{cmd:?} dispatching {cmd_shard} on {range_start}");

        match self.shards.get_shard(&shard_id) {
            Some(shard) => self.dispatch_local(shard.clone(), cmd).await,
            None => {
                println!(
                    "[reactor {}] shard {} not managed by this reactor (slot: {}, crc16: {}, cmd: {:?})",
                    self.reactor_metadata.id,
                    shard_id,
                    cmd_slot,
                    cmd.get_crc16(),
                    cmd
                );
                todo!(); // TODO: return a moved information
            }
        }
    }

    pub fn get_topology(&self) -> Option<Rc<Topology>> {
        return self.topology.borrow().clone();
    }
}
