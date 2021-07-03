use clap::{crate_authors, crate_version, Clap};
use kvs::KvStore;
use std::{path::Path, process};

#[derive(Clap)]
#[clap(version =crate_version!() , author = crate_authors!())]
struct Options {
    #[clap(subcommand)]
    subcmd: SubCommand,
}
#[derive(Clap)]
enum SubCommand {
    Get(Key),
    Set(KeyValue),
    RM(Key),
}
#[derive(Clap)]
struct Key {
    key: String,
}
#[derive(Clap)]
struct KeyValue {
    key: String,
    value: String,
}
fn main() {
    let opts = Options::parse();
    let mut kvs = KvStore::open(Path::new("")).unwrap();
    match opts.subcmd {
        SubCommand::Get(m) => match kvs.get(m.key) {
            Ok(Some(value)) => {
                println!("{}", value);
            }
            _ => {
                println!("Key not found");
                process::exit(0);
            }
        },
        SubCommand::RM(m) => match kvs.remove(m.key) {
            Ok(_) => {}
            _ => {
                println!("Key not found");
                process::exit(-1);
            }
        },
        SubCommand::Set(m) => match kvs.set(m.key, m.value) {
            Ok(_) => {}
            Err(e) => {
                println!("{}", e);
                process::exit(-1);
            }
        },
    }
}
