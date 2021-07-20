use std::{
    io::{BufReader, BufWriter, Write},
    net::{SocketAddr, TcpStream},
};

use serde_json::{de::IoRead, StreamDeserializer};

use crate::{
    error::{Error, ErrorKind, Result},
    net::{Request, Response},
};

pub struct Client<'a> {
    writer: BufWriter<TcpStream>,
    reader: StreamDeserializer<'a, IoRead<BufReader<TcpStream>>, Response>,
}

impl<'a> Client<'a> {
    pub fn connect(addr: SocketAddr) -> Result<Client<'a>> {
        let stream = TcpStream::connect(addr)?;
        let writer = BufWriter::new(stream.try_clone()?);
        let reader = BufReader::new(stream);
        let reader = serde_json::Deserializer::from_reader(reader).into_iter::<Response>();
        Ok(Client { reader, writer })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = Request::Get {
            key: key.to_owned(),
        };

        self.send_request(&request)?;

        if let Some(response) = self.reader.next() {
            if let Response::Get(Ok(result)) = response? {
                return Ok(result);
            }
        }

        Err(Error::from(ErrorKind::KeyNotFound(format!(
            "cannot get value of key:{}",
            key
        ))))
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Request::Set { key, value };
        self.send_request(&request)?;

        if let Some(response) = self.reader.next() {
            if let Response::Set(Ok(())) = response? {
                return Ok(());
            }
        }
        Err(Error::from(ErrorKind::Error(
            "cannot get response from server".to_string(),
        )))
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let request = Request::Remove { key };
        self.send_request(&request)?;

        if let Some(response) = self.reader.next() {
            if let Response::Remove(Ok(())) = response? {
                return Ok(());
            }
        }

        Err(Error::from(ErrorKind::Error(
            "cannot get response from server".to_string(),
        )))
    }

    fn send_request(&mut self, request: &Request) -> Result<()> {
        let buf = serde_json::to_vec(request)?;

        self.writer.write_all(&buf[..])?;
        self.writer.flush()?;

        Ok(())
    }
}
