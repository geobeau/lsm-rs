use futures::AsyncWriteExt;
use glommio::net::TcpListener;

use crate::{memcached::{MemcachedBinaryHandler, Command, Response}, datastore::DataStore};

use super::{Get, Set, GetResp, SetResp, OpCode};



pub struct MemcachedBinaryServer {
    pub host_port: String,
    pub storage: DataStore,
}


impl MemcachedBinaryServer {
    async fn handle_get(&self, g: Get) -> GetResp {
        let maybe_value = match self.storage.get(&g.key).await {
            Some(r) => Some(r.value),
            None => None,
        };
        return GetResp {
            flags: 0,
            opcode: OpCode::NoError,
            cas: 0,
            value: maybe_value,
        }
    }
    
    async fn handle_set(&self, s: Set) -> SetResp {
        return SetResp {
            opcode: OpCode::NoError,
            cas: 0,
        }
    }

    pub async fn listen(&self) {
        let listener = TcpListener::bind(self.host_port.clone()).unwrap();
        println!("Listening on {}", listener.local_addr().unwrap());
        loop {
            println!("Waiting request!");
            let mut stream = listener.accept().await.unwrap();
            let mut handler = MemcachedBinaryHandler { stream };
            loop {
                handler.await_new_data().await;
                let command = handler.decode_command().await.unwrap();
                println!("Processing something!");
                println!("Command: {:?}", command);
                let resp = match command {
                    Command::Set(c) => Response::Set(self.handle_set(c).await),
                    Command::Get(c) => Response::Get(self.handle_get(c).await),
                }.to_bytes();
        
                handler.write_resp(&resp).await;
                println!("Responded!");
            }

        }
    }


    
}
