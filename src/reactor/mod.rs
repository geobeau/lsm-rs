use std::{path::PathBuf, rc::Rc, time::Duration};

use monoio::{join, time::sleep};

use crate::{
    cluster::{Cluster, ClusteredReactor},
    memcached::server::MemcachedBinaryServer,
    redis::server::RESPServer,
    storageproxy::StorageProxy,
};

pub fn start_reactor(clustered_reactor: ClusteredReactor, cluster: Cluster, reactor_id: u8, data_dir: &PathBuf) {
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

        let storage_proxy = StorageProxy::new(reactor_id, &clustered_reactor, &cluster, data_dir).await;

        let resp = RESPServer {
            host_port: format!("127.0.0.1:{}", clustered_reactor.reactor.port),
            storage_proxy: storage_proxy.clone(),
            cluster,
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
