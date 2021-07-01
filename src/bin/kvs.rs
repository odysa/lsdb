use std::process;

use clap::{crate_authors, crate_version, App, Arg, ArgMatches, Clap};
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
            eprint!("unimplemented");
            process::exit(1);
        }
    }
}
