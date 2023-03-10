use std::path::PathBuf;

use rand::{Rng, thread_rng, distributions::Alphanumeric};
use lsm_rs::{datastore::{self, Config}, record::Record};

use criterion::{criterion_group, criterion_main, Criterion};


pub fn kvs_benchmark(c: &mut Criterion) {
    let dataset_10k = gen_dataset(10_000);
    let config = Config{ memtable_max_size_bytes: 4096, disktable_target_usage_ratio: 0.7 };
    c.bench_function("kvs: insert random 10K, 4kiB tables", |b| b.iter(|| kvs_insert(&dataset_10k, &config)));
    let config = Config{ memtable_max_size_bytes: 4096 * 1024, disktable_target_usage_ratio: 0.7 };
    c.bench_function("kvs: insert random 10K, 4MiB tables", |b| b.iter(|| kvs_insert(&dataset_10k, &config)));
    let config = Config{ memtable_max_size_bytes: 64 * 1024 * 1024, disktable_target_usage_ratio: 0.7 };
    c.bench_function("kvs: insert random 10K, 64MiB tables", |b| b.iter(|| kvs_insert(&dataset_10k, &config)));
}

fn gen_string() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect()
}

fn gen_dataset(n: usize) -> Vec<(String, String)> {
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        let t = (gen_string(), gen_string());
        v.push(t);
    }
    return v;
}

fn kvs_insert(dataset: &Vec<(String, String)>, conf: &Config) {
    let mut s = datastore::DataStore::new_with_config(PathBuf::from("./data/bench/"), conf.clone());

    dataset.into_iter().for_each(|(key,value)| {
        s.set(Record::new(key.clone(), value.clone()))
    });

    s.truncate();
}

criterion_group!(benches, kvs_benchmark);
criterion_main!(benches);