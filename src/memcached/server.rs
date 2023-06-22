use std::rc::Rc;

use glommio::net::TcpListener;

use crate::{
    datastore::DataStore,
    memcached::{Command, MemcachedBinaryHandler, Response},
    record::Record,
};

use super::{Get, GetResp, OpCode, Set, SetResp};

#[derive(Clone)]
pub struct MemcachedBinaryServer {
    pub host_port: String,
    pub storage: StorageProxy,
}

#[derive(Clone)]
pub struct StorageProxy {
    pub storage: Rc<DataStore>,
}

impl MemcachedBinaryServer {
    pub async fn listen(self) {
        let listener = TcpListener::bind(self.host_port.clone()).unwrap();
        println!("Listening on {}", listener.local_addr().unwrap());
        loop {
            println!("Waiting request!");
            let stream = listener.accept().await.unwrap();
            let storage = self.storage.clone();

            glommio::spawn_local(async move {
                let mut handler = MemcachedBinaryHandler { stream };
                println!("Spawned new task");

                loop {
                    handler.await_new_data().await;
                    let command = handler.decode_command().await.unwrap();
                    println!("Processing something!");
                    println!("Command: {:?}", command);
                    let resp = match command {
                        Command::Set(c) => Response::Set(storage.handle_set(c).await),
                        Command::Get(c) => Response::Get(storage.handle_get(c).await),
                    }
                    .to_bytes();

                    handler.write_resp(&resp).await;
                    println!("Responded!");
                }
            })
            .detach();
        }
    }
}

impl StorageProxy {
    async fn handle_set(&self, s: Set) -> SetResp {
        self.storage.set(Record::new(s.key, s.data)).await;
        SetResp {
            opcode: OpCode::NoError,
            cas: 0,
        }
    }
    async fn handle_get(&self, g: Get) -> GetResp {
        let maybe_value = match self.storage.get(&g.key).await {
            Some(r) => Some(r.value),
            None => None,
        };
        GetResp {
            flags: 0,
            opcode: OpCode::NoError,
            cas: 0,
            value: maybe_value,
        }
    }
}
