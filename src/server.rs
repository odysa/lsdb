use serde_json::Deserializer;
use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener, TcpStream};

use crate::common::{Command, KvsEngine};
use crate::error::Result;
pub struct Server<T: KvsEngine> {
    engine: T,
}

impl<T: KvsEngine> Server<T> {
    pub fn new(engine: T) -> Self {
        Server { engine }
    }

    pub fn server(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            self.handle_client(stream?)?;
        }
        Ok(())
    }

    fn handle_client(&self, stream: TcpStream) -> Result<()> {
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let requests = Deserializer::from_reader(reader).into_iter::<Command>();
        for request in requests {
            let request = request?;
            match request {
                Command::Get { key } => {
                    serde_json::to_writer(&mut writer, &Command::Get { key })?;
                }
                Command::Remove { key } => {
                    serde_json::to_writer(&mut writer, &Command::Remove { key })?;
                }
                Command::Set { key, value } => {
                    serde_json::to_writer(&mut writer, &Command::Set { key, value })?;
                }
            }
        }
        Ok(())
    }
}
