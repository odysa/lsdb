use assert_cmd::prelude::*;
use std::process::Command;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use kvs::{common::KvsEngine, kvs_store::KvStore};
use tempfile::TempDir;

// single thread store benchmark
pub fn store_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_bench");
    group.bench_with_input(BenchmarkId::new("kvs", 1), &100000, |b, i| {
        b.iter(|| {
            let dir = TempDir::new().unwrap();
            let path = dir.path();
            let kvs = KvStore::open(path).unwrap();
            for j in 0..*i {
                kvs.set(format!("key{}", j), format!("value{}", j)).unwrap();
                if j - 1 >= 0 {
                    kvs.get(format!("key{}", j)).unwrap();
                }
            }
        })
    });
}

pub fn server_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("server_bench");

    group.bench_with_input(BenchmarkId::new("kvs", 1), &10, |b, i| {
        b.iter(|| {
            for j in 0..*i {
                set(format!("key{}", j), format!("value{}", j));
                if j - 1 >= 0 {
                    get(format!("key{}", j));
                }
            }
        });
    });
}

pub fn set(key: String, value: String) {
    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["set", &key, &value])
        .assert()
        .success();
}

pub fn get(key: String) {
    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["get", &key])
        .assert()
        .success();
}

criterion_group!(benches, server_bench);
criterion_main!(benches);
