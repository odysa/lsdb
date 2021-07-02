use clap::{crate_authors, crate_version, App, Arg, ArgMatches, Clap};
use kvs::{log::Logger, KvStore};
use std::{
    path::{Path, PathBuf},
    process,
};

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
        SubCommand::Get(m) => {
            eprint!("unimplemented");
            process::exit(1);
        }
        SubCommand::RM(m) => {
            eprint!("unimplemented");
            process::exit(1);
        }
        SubCommand::Set(m) => {
            // eprint!("unimplemented");
            kvs.set(m.key, m.value).unwrap();
        }
    }
}

fn write() {
    let path = PathBuf::from("a.db");

    match Logger::new(path) {
        Ok(mut log) => match log.append("123".to_string()) {
            Ok(()) => {
                println!("ok");
            }
            Err(e) => {
                println!("{}", e);
            }
        },
        Err(e) => {
            println!("{}", e);
        }
    }
}
