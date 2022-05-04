# LSDB

A LSM based key-value Storage that supports compaction.

## run

Launch server
```
cargo run --bin kvs-server --addr 127.0.0.1:4000
```

launch client
```
cargo run --bin kvs-client set key value --addr 127.0.0.1:4000
```

available commands:
```
set key value
rm key
get key
```

## build
```
cargo build
```
## benchmark
```
cargo bench
```
## test
```
cargo test
```