use glommio::channels::channel_mesh::Full;
use glommio::channels::channel_mesh::MeshBuilder;

use glommio::prelude::*;


use glommio::timer::sleep;
use lsm_rs::storageproxy::{CommandHandle, StorageProxy};
use lsm_rs::{datastore::DataStore, memcached::server::MemcachedBinaryServer};
use std::rc::Rc;
use std::time::Duration;


pub fn start_compaction_manager(ds: Rc<DataStore>) {
    glommio::spawn_local(async move {
        loop {
            ds.maybe_run_one_reclaim().await;
            ds.get_stats().assert_not_corrupted();
            sleep(Duration::from_millis(100)).await
        }
    }).detach();
}

pub fn start_flush_manager(ds: Rc<DataStore>) {
    glommio::spawn_local(async move {
        loop {
            ds.flush_all_flushable_memtables().await;
            ds.clean_unused_disktables().await;
            sleep(Duration::from_millis(100)).await
        }
    }).detach();
}


fn main() {
    let nr_shards = 1;

    let mesh_builder: MeshBuilder<CommandHandle, Full> = MeshBuilder::full(nr_shards, 1024);

    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(nr_shards, None))
        .on_all_shards(move || async move {
            let id = glommio::executor().id();
            println!("Starting executor {}", id);
            let ds = Rc::from(DataStore::new("./data/".into()).await);
            println!("datastore inited");
            let storage_proxy: StorageProxy;
            start_compaction_manager(ds.clone());
            start_flush_manager(ds.clone());

            if nr_shards > 1 {
                println!("Mesh mode, initalizing mesh");
                let (sender, receiver) = mesh_builder.join().await.unwrap();
                let cur_shard = sender.peer_id();

                let sender = Rc::from(sender);

                storage_proxy = StorageProxy {
                    datastore: ds,
                    sender: Some(sender),
                    cur_shard,
                    nr_shards,
                };
                println!("Spawning mesh receivers");
                storage_proxy.spawn_remote_dispatch_handlers(receiver).await;
            } else {
                println!("Single core mode");
                storage_proxy = StorageProxy {
                    datastore: ds,
                    sender: None,
                    cur_shard: 0,
                    nr_shards,
                };
            }

            let s = MemcachedBinaryServer {
                host_port: "127.0.0.1:11211".to_string(),
                storage_proxy,
            };
            s.listen().await;
            println!("Terminated");
        })
        .unwrap()
        .join_all();
}
