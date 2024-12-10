use std::{rc::Rc, time::Duration};

use monoio::{join, time::sleep};

use crate::{cluster::{Cluster, ClusteredReactor}, datastore::DataStore, memcached::server::MemcachedBinaryServer, redis::server::RESPServer, storageproxy::StorageProxy};


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

pub fn start_stat_manager(ds: Rc<DataStore>, shard: u8) {
    monoio::spawn(async move {
        loop {
            let stats = ds.get_stats();
            println!("stats {shard}: {:?}", stats);
            sleep(Duration::from_millis(1000)).await
        }
    });
}


pub fn start_reactor(clustered_reactor: ClusteredReactor, cluster: Cluster, reactor_id: u8) {
    println!("Start reactor {reactor_id}");

    let urb = io_uring::IoUring::builder();
    // urb.setup_sqpoll(1000);
    // urb.setup_sqpoll_cpu(5);

    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .uring_builder(urb)
        .enable_timer()
        .with_entries(8192)
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
        start_stat_manager(ds.clone(), reactor_id);

        println!("Single core mode");
        storage_proxy = StorageProxy {
            datastore: ds,
            cur_shard: 0,
            nr_shards: 1,
        };
        let resp = RESPServer {
            host_port: format!("127.0.0.1:{}", clustered_reactor.reactor.port),
            storage_proxy: storage_proxy.clone(),
            cluster: cluster,
        };
        let memcached_port = 11211 + reactor_id as u64;
        let memcached = MemcachedBinaryServer {
            host_port: format!("127.0.0.1:{}", memcached_port),
            storage_proxy: storage_proxy.clone(),
        };
        
        join!(resp.listen(), memcached.listen());
        println!("Terminated");
    });
}