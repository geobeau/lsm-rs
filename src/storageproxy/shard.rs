use std::{path::PathBuf, rc::Rc, time::Duration};

use monoio::time::sleep;

use crate::datastore::DataStore;

pub fn start_compaction_manager(shard: Rc<Shard>) {
    monoio::spawn(async move {
        loop {
            shard.datastore.maybe_run_one_reclaim().await;
            shard.datastore.get_stats().assert_not_corrupted();
            sleep(Duration::from_millis(200)).await
        }
    });
}

pub fn start_flush_manager(shard: Rc<Shard>) {
    monoio::spawn(async move {
        loop {
            shard.datastore.flush_all_flushable_memtables().await;
            shard.datastore.clean_unused_disktables().await;
            sleep(Duration::from_millis(200)).await
        }
    });
}

pub fn start_stat_manager(shard: Rc<Shard>, reactor: u8) {
    monoio::spawn(async move {
        loop {
            let stats = shard.datastore.get_stats();
            println!("stats reactor:{reactor}: {:?}", stats);
            sleep(Duration::from_millis(1000)).await
        }
    });
}

pub struct Shard {
    pub datastore: DataStore,
}

impl Shard {
    pub async fn new(reactor_id: u8, data_dir: PathBuf) -> Rc<Shard> {
        let datastore = DataStore::new(data_dir).await;
        let shard = Rc::from(Shard { datastore });
        start_compaction_manager(shard.clone());
        start_flush_manager(shard.clone());
        start_stat_manager(shard.clone(), reactor_id);
        println!("datastore inited");
        shard
    }
}
