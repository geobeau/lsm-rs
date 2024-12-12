use futures::channel::mpsc;
use lsm_rs::cluster::ClusterManager;
use lsm_rs::topology::{self, LocalTopology, ReactorMetadata, Topology};
use lsm_rs::reactor::{ Reactor};
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::thread;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "lsm-rs", about = "lsm-rs is a (mostly) Redis compatible database")]
struct Opt {
    /// Number of shards for the given cluster
    #[structopt(short = "s", long = "shards", default_value = "8")]
    shard_total: u16,

    /// Number of reactors to start
    #[structopt(short = "r", long = "reactors", default_value = "2")]
    reactors_total: u16,

    /// Input file
    #[structopt(short = "d", long = "data-directory", parse(from_os_str), default_value = "./data/")]
    data_dir: std::path::PathBuf,
}

fn main() {
    let opt = Opt::from_args();

    // let cpus = CpuSet::online().unwrap();
    let mut shard_threads = vec![];
    let mut reactors = Vec::with_capacity(opt.reactors_total as usize);
    let mut reactor_metadatas = Vec::with_capacity(opt.reactors_total as usize);
    let mut port = 6379;
    let mut mesh: HashMap<u8, async_channel::Sender<Topology>> = HashMap::new();

    for reactor_id in 0..opt.reactors_total {
        let metadata = ReactorMetadata {
            id: reactor_id as u8,
            ip: std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        };
        reactor_metadatas.push(metadata.clone());

        let data_dir = opt.data_dir.clone();
        let (sender, receiver) = async_channel::unbounded();
        reactors.push(Reactor::new(metadata, receiver, data_dir));
        mesh.insert(reactor_id as u8, sender);
        port += 1;
    }

    let cm = ClusterManager::new(reactor_metadatas.clone(), opt.shard_total, mesh, None);
    reactors[0].cluster_manager(cm);


    println!("{:?}", opt.data_dir);
    

    for reactor in reactors {
        let t = thread::spawn(move || {
            reactor.start();
        });
        shard_threads.push(t);
    }

    for t in shard_threads {
        t.join().unwrap();
    }
}
