use std::collections::HashMap;

use crate::{
    api::{self, ClusterTopologyResp, Response},
    topology::{self, ReactorMetadata, Topology},
};

pub struct ClusterManager {
    mesh: HashMap<u8, async_channel::Sender<Topology>>,
    topology: Topology,
    receiver: async_channel::Receiver<ClusterMessage>,
}

pub struct ClusterMessage {
    pub response_chan: async_channel::Sender<Response>,
    pub command: api::ClusterCommand,
}

impl ClusterManager {
    pub fn new(
        local_reactors: Vec<ReactorMetadata>,
        shards_total: u16,
        mesh: HashMap<u8, async_channel::Sender<Topology>>,
        receiver: async_channel::Receiver<ClusterMessage>,
        contact_point: Option<String>,
    ) -> ClusterManager {
        let topology = match contact_point {
            Some(_) => todo!(),
            None => ClusterManager::init_topology(local_reactors, shards_total),
        };

        ClusterManager { mesh, topology, receiver }
    }

    fn init_topology(local_reactors: Vec<ReactorMetadata>, shards_total: u16) -> Topology {
        topology::Topology::new_with_reactors(shards_total, local_reactors)
    }

    pub async fn start(&mut self) {
        self.broadcast_topology().await;
        loop {
            let msg = self.receiver.recv().await.unwrap();
            match msg.command {
                api::ClusterCommand::Join(join) => self.join_new_node(join.reactors),
            }
            msg.response_chan
                .send(Response::ClusterTopology(ClusterTopologyResp {
                    topology: self.topology.clone(),
                }))
                .await
                .unwrap();
            self.broadcast_topology().await;
        }
    }

    fn join_new_node(&mut self, new_reactors: Vec<ReactorMetadata>) {
        self.topology.add_reactors(new_reactors);
        self.topology.rebalance();
    }

    async fn broadcast_topology(&self) {
        println!("{:?}", self.topology);
        for (_, local_peer) in &self.mesh {
            local_peer.send(self.topology.clone()).await.unwrap();
        }
    }
}
