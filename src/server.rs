use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener, TcpStream};

use serde_json::Deserializer;

use crate::common::KvsEngine;
use crate::error::Result;
struct Server<T: KvsEngine> {
    engine: T,
}

impl<T: KvsEngine> Server<T> {
    pub fn new(engine: T) -> Self {
        Server { engine }
    }

    pub fn server(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            self.handle_client(stream?);
        }
        Ok(())
    }

    fn handle_client(&self, stream: TcpStream) {
        let reader = BufReader::new(&stream);
        let writer = BufWriter::new(&stream);
        let requests = Deserializer::from_reader(reader).into_iter();
    }
}
