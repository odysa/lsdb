# Yet another kvs store

## build
```rust
cargo build
```

## run
Launch server first
```
cargo run --bin kvs-server --addr 127.0.0.1:4000
```
then launch client
```
cargo run --bin kvs-client set key value --addr 127.0.0.1:4000
```

available commands:
```
set key value
rm key
get key
```