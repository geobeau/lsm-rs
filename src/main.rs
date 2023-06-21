use glommio::prelude::*;
use lsm_rs::{datastore::DataStore, memcached::server::MemcachedBinaryServer, record::Record};

fn main() {
    let ex = LocalExecutorBuilder::new(Placement::Fixed(0)).make().unwrap();
    ex.run(async move {
        let ds = DataStore::new("./data/".into()).await;
        ds.set(Record::new("titi".to_string(), Vec::from("toto".as_bytes()))).await;
        println!("{:?}", ds.get("titi").await.unwrap());

        let s = MemcachedBinaryServer {
            host_port: "127.0.0.1:11211".to_string(),
            storage: ds,
        };
        s.listen().await;
    });
}
