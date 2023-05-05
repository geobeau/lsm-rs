use glommio::{net::TcpListener, prelude::*};
use lsm_rs::{datastore::DataStore, memcached::{MemcachedBinaryHandler, server::MemcachedBinaryServer, Get, GetResp, SetResp, Set}, record::Record};

fn handle_get(g: Get) -> GetResp {
    return GetResp {
        flags: 0,
    }
}

fn handle_set(s: Set) -> SetResp {
    return SetResp {
        opcode: lsm_rs::memcached::OpCode::NoError,
        cas: 0,
    }
}



fn main() {
    let ex = LocalExecutorBuilder::new(Placement::Fixed(0)).make().unwrap();
    ex.run(async move {
        let mut s = DataStore::new("./data/".into()).await;
        s.set(Record::new("test".to_string(), Vec::from("test".as_bytes())));
        println!("{:?}", s.get("test").await.unwrap());
    });



    ex.run(async move {
        let s = MemcachedBinaryServer {
            host_port: "127.0.0.1:11211".to_string(),
            get_handler: handle_get,
            set_handler: handle_set,
        };
        s.listen().await;
    });
}
