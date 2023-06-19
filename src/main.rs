use glommio::{net::TcpListener, prelude::*};
use lsm_rs::{datastore::DataStore, memcached::{MemcachedBinaryHandler, server::MemcachedBinaryServer, Get, GetResp, SetResp, Set}, record::Record};



fn main() {
    let ex = LocalExecutorBuilder::new(Placement::Fixed(0)).make().unwrap();
    ex.run(async move {
        let mut ds = DataStore::new("./data/".into()).await;
        ds.set(Record::new("titi".to_string(), Vec::from("toto".as_bytes())));
        println!("{:?}", ds.get("titi").await.unwrap());

        let s = MemcachedBinaryServer {
            host_port: "127.0.0.1:11211".to_string(),
            storage: ds,
        };
        s.listen().await;
    });
}
