use assert_cmd::prelude::*;
use std::sync::mpsc;
use std::thread;
use std::{process::Command, time::Duration};

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
            let mut kvs = KvStore::open(path).unwrap();
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

    group.bench_with_input(BenchmarkId::new("kvs", 1), &100000, |b, i| {
        // let (sender, receiver) = mpsc::sync_channel::<i32>(0);
        // let temp_dir = TempDir::new().unwrap();
        // let mut server = Command::cargo_bin("kvs-server").unwrap();
        // let mut child = server.current_dir(&temp_dir).spawn().unwrap();

        // let handle = thread::spawn(move || {
        //     // wait for main thread to finish
        //     let _ = receiver.recv();
        //     child.kill().expect("server exited before killed");
        // });

        b.iter(|| {
            for j in 0..*i {
                set(format!("key{}", j), format!("value{}", j));
                if j - 1 >= 0 {
                    get(format!("key{}", j));
                }
            }
        });
        // println!("do");
        // sender.send(0).unwrap();
        // handle.join().unwrap();
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
