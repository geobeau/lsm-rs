use std::rc::Rc;

use glommio::prelude::*;
use lsm_rs::{
    datastore::DataStore,
    memcached::server::{MemcachedBinaryServer, StorageProxy},
};

fn main() {
    let ex = LocalExecutorBuilder::new(Placement::Fixed(0)).make().unwrap();
    ex.run(async move {
        let ds = DataStore::new("./data/".into()).await;

        let s = MemcachedBinaryServer {
            host_port: "127.0.0.1:11211".to_string(),
            storage: StorageProxy { storage: Rc::from(ds) },
        };
        s.listen().await;
    });
}
