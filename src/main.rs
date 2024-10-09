use lsm_rs::memcached::server::MemcachedBinaryServer;
use lsm_rs::redis::server::RESPServer;
use lsm_rs::storageproxy::StorageProxy;
use lsm_rs::datastore::DataStore;
use monoio::join;
use monoio::time::sleep;
use std::rc::Rc;
use std::time::Duration;


pub fn start_compaction_manager(ds: Rc<DataStore>) {
    monoio::spawn(async move {
        loop {
            ds.maybe_run_one_reclaim().await;
            ds.get_stats().assert_not_corrupted();
            sleep(Duration::from_millis(200)).await
        }
    });
}

pub fn start_flush_manager(ds: Rc<DataStore>) {
    monoio::spawn(async move {
        loop {
            ds.flush_all_flushable_memtables().await;
            ds.clean_unused_disktables().await;
            sleep(Duration::from_millis(200)).await
        }
    });
}


fn main() {
    let nr_shards = 1;
    // let cpus = CpuSet::online().unwrap();

    // let mesh_builder: MeshBuilder<CommandHandle, Full> = MeshBuilder::full(nr_shards, 1024);
    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .with_entries(4096)
        .enable_timer()
        .build()
        .unwrap();

    rt.block_on(async {
            let id = 0;
            println!("Starting executor {}", id);
            let ds = Rc::from(DataStore::new("./data/".into()).await);
            println!("datastore inited");
            let storage_proxy: StorageProxy;
            start_compaction_manager(ds.clone());
            start_flush_manager(ds.clone());

            if nr_shards > 1 {
                println!("Mesh mode, initalizing mesh");
                // let (sender, receiver) = mesh_builder.join().await.unwrap();
                let cur_shard = 0;

                // let sender = Rc::from(sender);

                storage_proxy = StorageProxy {
                    datastore: ds,
                    // sender: Some(sender),
                    cur_shard,
                    nr_shards,
                };
                println!("Spawning mesh receivers");
                // storage_proxy.spawn_remote_dispatch_handlers(receiver).await;
            } else {
                println!("Single core mode");
                storage_proxy = StorageProxy {
                    datastore: ds,
                    // sender: None,
                    cur_shard: 0,
                    nr_shards,
                };
            }

            let resp = RESPServer {
                host_port: "127.0.0.1:6379".to_string(),
                storage_proxy: storage_proxy.clone(),
            };
            let memcached = MemcachedBinaryServer {
                host_port: "127.0.0.1:11211".to_string(),
                storage_proxy: storage_proxy.clone(),
            };
            
            join!(resp.listen(), memcached.listen());
            println!("Terminated");
        });
}
