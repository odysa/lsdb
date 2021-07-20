use crate::common::KvsEngine;
use crate::error::Result;
use crate::net::{Request, Response};
use crate::thread_pool::{QueueThreadPool, ThreadPool};
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
        let pool = QueueThreadPool::new(10)?;
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
        let requests = Deserializer::from_reader(reader).into_iter::<Request>();

        for request in requests {
            if let Ok(request) = request {
                info!(logger,"request:"; "request" => format!("{:?}", request));

                let response = match request {
                    Request::Get { key } => match self.engine.get(key) {
                        Ok(v) => Response::Get(Ok(v)),
                        Err(e) => Response::Get(Err(e.to_string())),
                    },
                    Request::Remove { key } => match self.engine.remove(key) {
                        Ok(_) => Response::Remove(Ok(())),
                        Err(e) => Response::Remove(Err(e.as_string())),
                    },
                    Request::Set { key, value } => match self.engine.set(key, value) {
                        Ok(()) => Response::Set(Ok(())),
                        Err(e) => Response::Set(Err(e.to_string())),
                    },
                };

                self.send_response(&mut writer, &response)?;

                debug!(
                    logger,
                    "Response sent to";
                    "addr" => format!("{}",peer_addr),
                    "response" => format!("{:?}",response)
                );
                // write response
            } else {
                error!(logger, "can not parse the request");
            }
        }
        Ok(())
    }

    fn send_response(&self, writer: &mut BufWriter<&TcpStream>, response: &Response) -> Result<()> {
        let buf = serde_json::to_vec(response)?;
        writer.write_all(&buf[..])?;
        writer.flush()?;
        Ok(())
    }
}
