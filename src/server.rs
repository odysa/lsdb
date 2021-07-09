use crate::common::{Command, GetResponse, KvsEngine, RemoveResponse, SetResponse};
use crate::error::Result;
use serde_json::Deserializer;
use slog::{debug, error, info, Logger};
use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
pub struct Server<T: KvsEngine> {
    engine: T,
}

impl<T: KvsEngine> Server<T> {
    pub fn new(engine: T) -> Self {
        Server { engine }
    }

    pub fn serve(&mut self, addr: &SocketAddr, logger: &Logger) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            self.handle_client(stream?, logger)?;
        }
        Ok(())
    }

    fn handle_client(&mut self, stream: TcpStream, logger: &Logger) -> Result<()> {
        let peer_addr = stream.peer_addr()?;
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let requests = Deserializer::from_reader(reader).into_iter::<Command>();

        macro_rules! respond {
            ($response: expr) => {{
                let response = $response;
                serde_json::to_writer(&mut writer, &response)?;
                writer.flush()?;
                debug!(
                    logger,
                    "Response sent to";
                    "addr" => format!("{}",peer_addr),
                    "response" => format!("{:?}",response)
                );
            };};
        }

        for request in requests {
            if let Ok(request) = request {
                info!(logger,"request:"; "request" => format!("{:?}", request));
                match request {
                    Command::Get { key } => {
                        respond!(match self.engine.get(key) {
                            Ok(v) => GetResponse::Ok(v),
                            Err(e) => GetResponse::Err(e.as_string()),
                        })
                    }
                    Command::Remove { key } => {
                        respond!(match self.engine.remove(key) {
                            Ok(_) => RemoveResponse::Ok(()),
                            Err(e) => RemoveResponse::Err(e.as_string()),
                        })
                    }
                    Command::Set { key, value } => {
                        respond!(match self.engine.set(key, value) {
                            Ok(()) => SetResponse::Ok(()),
                            Err(e) => SetResponse::Err(e.as_string()),
                        })
                    }
                }
                writer.flush()?;
            } else {
                error!(logger, "can not parse the request");
            }
        }
        Ok(())
    }
}
