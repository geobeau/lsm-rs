use std::rc::Rc;


use crate::{
    api::{Command, DeleteResp, GetResp, Response, SetResp},
    datastore::DataStore,
    record::HashedKey,
};

#[derive(Clone)]
pub struct StorageProxy {
    pub datastore: Rc<DataStore>,
    // pub sender: Option<Rc<Senders<CommandHandle>>>,
    pub cur_shard: usize,
    pub nr_shards: usize,
}

#[derive(Debug)]
pub struct CommandHandle {
    pub command: Command,
    // pub sender: SharedSender<Response>,
}

impl StorageProxy {
    fn get_shard_from_hash(&self, hash: &HashedKey) -> usize {
        hash[0] as usize % self.nr_shards
    }

    pub async fn dispatch_local(&self, cmd: Command) -> Response {
        match cmd {
            Command::Get(c) => {
                let record = self.datastore.get(&c.key).await;
                Response::Get(GetResp { record })
            }
            Command::Delete(c) => {
                self.datastore.delete(&c.key);
                Response::Delete(DeleteResp {})
            }
            Command::Set(c) => {
                self.datastore.set(c.record);
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
        let shard_id = self.get_shard_from_hash(cmd.get_hash());
        if self.cur_shard == shard_id {
            self.dispatch_local(cmd).await
        } else {
            panic!("Dispatch disabled")
            // self.dispatch_remote(cmd, shard_id).await
        }
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
