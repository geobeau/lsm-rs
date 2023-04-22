use glommio::{net::TcpListener, prelude::*};
use lsm_rs::{datastore::DataStore, memcached::MemcachedBinaryHandler, record::Record};

fn main() {
    let ex = LocalExecutorBuilder::new(Placement::Fixed(0)).make().unwrap();
    ex.run(async move {
        let mut s = DataStore::new("./data/".into()).await;
        s.set(Record::new("test".to_string(), Vec::from("test".as_bytes())));
        println!("{:?}", s.get("test").await.unwrap());
    });

    ex.run(async move {
        let listener = TcpListener::bind("127.0.0.1:11211").unwrap();
        println!("Listening on {}", listener.local_addr().unwrap());
        let stream = listener.accept().await.unwrap();
        let mut handler = MemcachedBinaryHandler { reader: stream };
        let command = handler.decode_command().await;
        println!("{:?}", command);
        let stream = listener.accept().await.unwrap();
        let mut handler = MemcachedBinaryHandler { reader: stream };
        let command = handler.decode_command().await;
        println!("{:?}", command);
    });
}
