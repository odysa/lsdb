use crate::common::KvsEngine;
use crate::error::Result;
use crate::net::{Request, Response};
use crate::thread_pool::ThreadPool;
use serde_json::Deserializer;
use slog::{error, info, o, Logger};
use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
pub struct Server<T: KvsEngine, U: ThreadPool> {
    engine: T,
    pool: U,
}

impl<T: KvsEngine, U: ThreadPool> Server<T, U> {
    pub fn new(engine: T, pool: U) -> Self {
        Server { engine, pool }
    }

    pub fn serve(&mut self, addr: &SocketAddr, logger: Logger) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        let logger = Arc::new(logger);

        for stream in listener.incoming() {
            if let Ok(stream) = stream {
                let peer_addr = stream.peer_addr()?;
                let engine = self.engine.clone();
                let logger = Arc::clone(&logger);

                self.pool.execute(move || {
                    let logger = logger.new(o!("peer_address"=>peer_addr));
                    if let Err(e) = handle_client(engine, stream, &logger) {
                        error!(logger, "Error on server"; "error" => format!("{}",e));
                    }
                })?;
            } else {
                error!(logger, "Error connection");
            }
        }

        Ok(())
    }
}

fn send_response(writer: &mut BufWriter<&TcpStream>, response: &Response) -> Result<()> {
    let buf = serde_json::to_vec(response)?;
    writer.write_all(&buf[..])?;
    writer.flush()?;
    Ok(())
}

fn handle_client<T: KvsEngine>(engine: T, stream: TcpStream, logger: &Logger) -> Result<()> {
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let requests = Deserializer::from_reader(reader).into_iter::<Request>();

    for request in requests {
        if let Ok(request) = request {
            info!(logger,"request:"; "request" => format!("{:?}", request));

            let response = match request {
                Request::Get { key } => match engine.get(key) {
                    Ok(v) => Response::Get(Ok(v)),
                    Err(e) => Response::Get(Err(e.to_string())),
                },
                Request::Remove { key } => match engine.remove(key) {
                    Ok(_) => Response::Remove(Ok(())),
                    Err(e) => Response::Remove(Err(e.as_string())),
                },
                Request::Set { key, value } => match engine.set(key, value) {
                    Ok(()) => Response::Set(Ok(())),
                    Err(e) => Response::Set(Err(e.to_string())),
                },
            };

            send_response(&mut writer, &response)?;

            info!(
                logger,
                "Response sent";
                "response" => format!("{:?}",response)
            );
            // write response
        } else {
            error!(logger, "can not parse the request");
        }
    }
    Ok(())
}
