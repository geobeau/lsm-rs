use std::{collections::HashMap, path::PathBuf, rc::Rc};

use crate::{
    api::{Command, DeleteResp, GetResp, Response, SetResp},
    cluster::{self, Cluster, ClusteredReactor},
    datastore::DataStore,
};

#[derive(Clone)]
pub struct StorageProxy {
    pub shards: HashMap<u16, Rc<DataStore>>,
    pub shards_count: u16,
}

#[derive(Debug)]
pub struct CommandHandle {
    pub command: Command,
    // pub sender: SharedSender<Response>,
}

impl StorageProxy {
    pub async fn new(clustered_reactor: &ClusteredReactor, cluster: &Cluster, data_dir: &PathBuf) -> StorageProxy {
        let mut proxy = StorageProxy {
            shards: HashMap::new(),
            shards_count: cluster.shards_count,
        };

        for slot in &clustered_reactor.shards {
            let mut shard_path = PathBuf::new();
            shard_path.push(format!("{}", slot.start));
            proxy.add_shard(slot.start, data_dir.join(shard_path)).await
        }

        proxy
    }

    pub async fn dispatch_local(&self, datastore: Rc<DataStore>, cmd: Command) -> Response {
        match cmd {
            Command::Get(c) => {
                let record = datastore.get(&c.key).await;
                Response::Get(GetResp { record })
            }
            Command::Delete(c) => {
                datastore.delete(&c.key);
                Response::Delete(DeleteResp {})
            }
            Command::Set(c) => {
                datastore.set(c.record);
                Response::Set(SetResp {})
            }
        }
    }

    // pub async fn dispatch_remote(&self, cmd: Command, shard_id: usize) -> Response {
    //     // println!("Dispatching to {}, from {} (of {})", shard_id, self.cur_shard, self.nr_shards);
    //     let sender = self.sender.as_ref().unwrap();
    //     let (resp_sender, resp_receiver) = shared_channel::new_bounded(1);
    //     sender
    //         .send_to(
    //             shard_id,
    //             CommandHandle {
    //                 command: cmd,
    //                 sender: resp_sender,
    //             },
    //         )
    //         .await
    //         .unwrap();
    //     resp_receiver.connect().await.recv().await.unwrap()
    // }

    pub async fn dispatch(&self, cmd: Command) -> Response {
        let cmd_shard = cmd.get_shard();
        let shard_id = cluster::compute_shard_id(cmd_shard, self.shards_count);
        // println!("{cmd:?} dispatching {cmd_shard} on {range_start}");
        match self.shards.get(&shard_id) {
            Some(ds) => self.dispatch_local(ds.clone(), cmd).await,
            None => {
                println!("shard {} not managed by this reactor (crc16: {}, cmd: {:?})", shard_id, cmd_shard, cmd);
                todo!(); // TODO: return a moved information
            }
        }
    }

    pub async fn add_shard(&mut self, range_start: u16, directory: PathBuf) {
        self.shards.insert(range_start, Rc::from(DataStore::new(directory).await));
    }

    // pub async fn spawn_remote_dispatch_handlers(&self, mut receiver: Receivers<CommandHandle>) {
    //     for (_i, stream) in receiver.streams() {
    //         let sp = self.clone();
    //         monoio::spawn(async move {
    //             // let result stream.recv().await
    //             while let Some(handle) = stream.recv().await {
    //                 let local_proxy = sp.clone();
    //                 monoio::spawn(async move { local_proxy.handle_command(handle).await });
    //             }
    //             panic!("Stop dispatcher");
    //         });
    //     }
    // }

    // pub async fn handle_command(&self, handle: CommandHandle) {
    //     let sender = handle.sender.connect().await;
    //     sender.send(self.dispatch_local(handle.command).await).await.unwrap();
    // }
}
