use futures::AsyncWriteExt;
use glommio::net::TcpListener;

use crate::memcached::{MemcachedBinaryHandler, Command, Response};

use super::{Get, Set, GetResp, SetResp};



pub struct MemcachedBinaryServer {
    pub host_port: String,
    pub get_handler: fn (Get) -> GetResp,
    pub set_handler: fn (Set) -> SetResp,
}

impl MemcachedBinaryServer {
    pub async fn listen(&self) {
        let listener = TcpListener::bind(self.host_port.clone()).unwrap();
        println!("Listening on {}", listener.local_addr().unwrap());
        loop {
            let mut stream = listener.accept().await.unwrap();
            let mut handler = MemcachedBinaryHandler { stream };
            let command = handler.decode_command().await.unwrap();
            println!("{:?}", command);
            let resp = match command {
                Command::Set(c) => Response::Set((self.set_handler)(c)),
                Command::Get(c) => Response::Get((self.get_handler)(c)),
            }.to_bytes();
    
            handler.write_resp(&resp).await;
        }
    }
}
