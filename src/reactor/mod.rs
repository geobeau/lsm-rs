use std::{path::PathBuf, rc::Rc};

use monoio::join;

use crate::{
    cluster::{ClusterManager, ClusterMessage},
    memcached::server::MemcachedBinaryServer,
    redis::server::RESPServer,
    storageproxy::StorageProxy,
    topology::{ReactorMetadata, Topology},
};

pub struct TopologyUpdater {
    receiver: async_channel::Receiver<Topology>,
    storage_proxy: Rc<StorageProxy>,
}

impl TopologyUpdater {
    pub async fn start(&self) {
        loop {
            println!("Waiting for new topology");
            let topology = self.receiver.recv().await.unwrap();
            println!("Received new topology");
            self.storage_proxy.apply_new_topology(&topology).await;
        }
    }
}

pub struct Reactor {
    metadata: ReactorMetadata,
    receiver: async_channel::Receiver<Topology>,
    data_dir: PathBuf,
    cm: Option<ClusterManager>,
    shard_total: u16,
    cluster_sender: async_channel::Sender<ClusterMessage>,
}

impl Reactor {
    pub fn new(
        reactor: ReactorMetadata,
        shard_total: u16,
        receiver: async_channel::Receiver<Topology>,
        cluster_sender: async_channel::Sender<ClusterMessage>,
        data_dir: PathBuf,
    ) -> Reactor {
        Reactor {
            metadata: reactor,
            receiver,
            data_dir,
            cluster_sender,
            cm: None,
            shard_total,
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
                }
                None => (),
            };

            let storage_proxy = Rc::from(StorageProxy::new(
                self.metadata.clone(),
                self.shard_total,
                self.cluster_sender.clone(),
                &self.data_dir,
            ));

            let topology_updated = TopologyUpdater {
                receiver: self.receiver.clone(),
                storage_proxy: storage_proxy.clone(),
            };

            let resp = RESPServer {
                host_port: format!("127.0.0.1:{}", self.metadata.port),
                storage_proxy: storage_proxy.clone(),
            };
            let memcached_port = 11211 + self.metadata.id as u64;
            let memcached = MemcachedBinaryServer {
                host_port: format!("127.0.0.1:{}", memcached_port),
                storage_proxy: storage_proxy.clone(),
            };

            join!(resp.listen(), memcached.listen(), topology_updated.start());
            println!("Terminated");
        });
    }
}
