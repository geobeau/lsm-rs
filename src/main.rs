use glommio::prelude::*;
use lsm_rs::{datastore::DataStore, record::Record};

fn main() {
    let ex = LocalExecutorBuilder::new(Placement::Fixed(0)).make().unwrap();
    ex.run(async move {
        let mut s = DataStore::new("./data/".into()).await;
        s.set(Record::new("test".to_string(), "test".to_string()));
    });
}
