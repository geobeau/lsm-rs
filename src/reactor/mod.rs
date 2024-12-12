use std::{borrow::Borrow, path::PathBuf, rc::Rc, time::Duration};

use monoio::{join, time::sleep};

use crate::{
    cluster::ClusterManager, memcached::server::MemcachedBinaryServer, redis::server::RESPServer, storageproxy::StorageProxy, topology::{LocalTopology, ReactorMetadata, Topology}
};

pub struct Reactor {
    metadata: ReactorMetadata,
    receiver: async_channel::Receiver<Topology>,
    data_dir: PathBuf,
    cm: Option<ClusterManager>
}

impl Reactor {
    pub fn new(reactor: ReactorMetadata, receiver: async_channel::Receiver<Topology>, data_dir: PathBuf) -> Reactor {
        Reactor {
            metadata: reactor,
            receiver,
            data_dir,
            cm: None,
        }

    }

    pub fn cluster_manager(&mut self, cm: ClusterManager) {
        self.cm = Some(cm);
    }


    pub fn start(&self) {
        println!("Start reactor {}", self.metadata.id);

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


            match &self.cm {
                Some(cm) => {
                    cm.start().await;
                },
                None => (),
            };

            let topology = self.receiver.recv().await.unwrap();
            let local_shards = topology.reactor_allocations.get(&self.metadata).unwrap();

            let storage_proxy = StorageProxy::new(self.metadata.id, local_shards, &topology, &self.data_dir).await;

            let resp = RESPServer {
                host_port: format!("127.0.0.1:{}", self.metadata.port),
                storage_proxy: storage_proxy.clone(),
                topology,
            };
            let memcached_port = 11211 + self.metadata.id as u64;
            let memcached = MemcachedBinaryServer {
                host_port: format!("127.0.0.1:{}", memcached_port),
                storage_proxy: storage_proxy.clone(),
            };

            join!(resp.listen(), memcached.listen());
            println!("Terminated");
        });
    }
}

