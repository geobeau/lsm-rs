use std::collections::HashMap;

use crate::topology::{self, ReactorMetadata, Topology};

pub struct ClusterManager {
    mesh: HashMap<u8, async_channel::Sender<Topology>>,
    topology: Topology,
}

impl ClusterManager {
    pub fn new(
        local_reactors: Vec<ReactorMetadata>,
        shards_total: u16,
        mesh: HashMap<u8, async_channel::Sender<Topology>>,
        contact_point: Option<String>,
    ) -> ClusterManager {
        let topology = match contact_point {
            Some(_) => todo!(),
            None => ClusterManager::init_topology(local_reactors, shards_total),
        };

        ClusterManager { mesh, topology }
    }

    fn init_topology(local_reactors: Vec<ReactorMetadata>, shards_total: u16) -> Topology {
        topology::Topology::new_with_reactors(shards_total, local_reactors)
    }

    pub async fn start(&self) {
        self.broadcast_topology().await;
    }

    async fn broadcast_topology(&self) {
        println!("{:?}", self.topology);
        for (_, local_peer) in &self.mesh {
            local_peer.send(self.topology.clone()).await.unwrap();
        }
    }
}
