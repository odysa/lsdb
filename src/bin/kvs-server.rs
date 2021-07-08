use clap::{crate_authors, crate_version, Clap, Error, ErrorKind};
use kvs::{kvs_store::KvStore, server::Server};
use slog::*;
use std::{env::current_dir, fs, net::SocketAddr, process::exit, str::FromStr};

#[derive(Clap)]
#[clap(version =crate_version!() , author = crate_authors!())]
struct Options {
    #[clap(long, short, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[clap(short, long, default_value = "kvs")]
    engine: Engine,
}
#[derive(Debug, PartialEq, Eq)]
enum Engine {
    Kvs,
    Sled,
}

impl FromStr for Engine {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "kvs" => Ok(Engine::Kvs),
            "sled" => Ok(Engine::Sled),
            _ => Err(Error::with_description(
                "engine should be either kvs or sled".to_string(),
                ErrorKind::InvalidValue,
            )),
        }
    }
}
impl Engine {
    fn to_string(&self) -> String {
        match self {
            &Engine::Kvs => "kvs".to_string(),
            &Engine::Sled => "sled".to_string(),
        }
    }
}
fn main() {
    let logger = logger();
    let options = Options::parse();
    let addr = options.addr;
    let engine = options.engine;
    let res = current_engine(&logger).and_then(|e| {
        // not target engine
        if e.is_some() && engine != e.unwrap() {
            error!(&logger, "Wrong engine!");
            exit(1);
        }
        run(&engine, &addr, &logger)
    });

    if let Err(e) = res {
        error!(&logger, "{}", e);
        exit(1);
    }
}

fn logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    slog::Logger::root(drain, o!())
}

fn run(engine: &Engine, addr: &SocketAddr, logger: &Logger) -> Result<()> {
    info!(logger, "YaKvs initializing";
        "version" => crate_version!(),
        "engine" => engine.to_string(),
         "ip" => addr
    );
    fs::write(
        current_dir()?.join("engine"),
        format!("{}", engine.to_string()),
    )?;

    match engine {
        Engine::Kvs => Ok(()),
        Engine::Sled => Ok(()),
    }
}

fn current_engine(logger: &Logger) -> Result<Option<Engine>> {
    let path = current_dir()?.join("engine");
    if !path.exists() {
        return Ok(None);
    }

    match fs::read_to_string(path)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(_) => {
            warn!(logger, "unable to read engine");
            Ok(None)
        }
    }
}
