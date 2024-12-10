use lsm_rs::reactor::start_reactor;
use lsm_rs::cluster::{self, ClusteredReactor, Reactor};
use structopt::StructOpt;
use std::net::Ipv4Addr;
use std::thread;

#[derive(Debug, StructOpt)]
#[structopt(name = "lsm-rs", about = "lsm-rs is a (mostly) Redis compatible database")]
struct Opt {
    /// Number of shards for the given cluster
    #[structopt(short = "s", long = "shards", default_value = "256")]
    shard_total: u16,

    /// Number of reactors to start
    #[structopt(short = "r", long = "reactors", default_value = "1")]
    reactors_total: u16,

    /// Input file
    #[structopt(parse(from_os_str), default_value = "./data/")]
    data_dir: std::path::PathBuf,
}



fn main() {
    let opt = Opt::from_args();
    
    // let cpus = CpuSet::online().unwrap();
    let mut shard_threads = vec![];
    let mut reactors = Vec::with_capacity(opt.reactors_total as usize);
    let mut port = 6379;

    for _ in 0..opt.reactors_total {
        reactors.push(Reactor {
            ip: std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        });
        port += 1;
    }

    let cluster = cluster::Cluster::new_with_reactors(opt.shard_total, reactors);

    let mut reactor_id = 0;
    for (reactor, ranges) in cluster.clone().reactor_allocations {
        let cluster = cluster.clone();

        let t = thread::spawn(move || {
            start_reactor(ClusteredReactor {
                reactor,
                ranges,
            }, cluster, reactor_id);
        });
        shard_threads.push(t);
        reactor_id += 1
    }
    
    for t in shard_threads {
        t.join();
    }
}
