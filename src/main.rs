use lsm_rs::memcached::server::MemcachedBinaryServer;
use lsm_rs::redis::server::RESPServer;
use lsm_rs::storageproxy::StorageProxy;
use lsm_rs::datastore::DataStore;
use lsm_rs::topology;
use monoio::join;
use monoio::time::sleep;
use std::rc::Rc;
use std::time::Duration;
use std::thread;


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


fn main() {
    let nr_shards = 3;
    // let cpus = CpuSet::online().unwrap();
    let mut shard_threads = vec![];

    let tology = topology::Topology::new_with_shard(nr_shards, 6379);

    for shard_id in 0..nr_shards {
        let local_topology = tology.clone();
        let t = thread::spawn(move || {
            println!("Start shard {shard_id}");

            let mut urb = io_uring::IoUring::builder();
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
                start_stat_manager(ds.clone(), shard_id);

                println!("Single core mode");
                storage_proxy = StorageProxy {
                    datastore: ds,
                    cur_shard: 0,
                    nr_shards: 1,
                };

                let resp_port = local_topology.shards[&shard_id].port;

                let resp = RESPServer {
                    host_port: format!("127.0.0.1:{}", resp_port),
                    storage_proxy: storage_proxy.clone(),
                    topology: local_topology,
                };
                let memcached_port = 11211 + shard_id as u64;
                let memcached = MemcachedBinaryServer {
                    host_port: format!("127.0.0.1:{}", memcached_port),
                    storage_proxy: storage_proxy.clone(),
                };
                
                join!(resp.listen(), memcached.listen());
                println!("Terminated");
            });
        });
        shard_threads.push(t)
    }
    
    for t in shard_threads {
        t.join();
    }
}
