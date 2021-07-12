use std::io::Cursor;

use bytes::{buf, Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::error::{Error, Result};

use super::protocol::Frame;

struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    cursor: usize,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4096),
            cursor: 0,
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse()? {
                return Ok(Some(frame));
            }

            // resise the buffer if it's full
            if self.buffer.len() == self.cursor {
                self.buffer.resize(self.cursor * 2, 0)
            }
            // read into buffer
            let num = self.stream.read(&mut self.buffer[self.cursor..]).await?;
            if num == 0 {
                return if self.cursor == 0 {
                    Ok(None)
                } else {
                    Err(Error::from("invalid connection".to_string()))
                };
            } else {
                self.cursor += num;
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.put_new_line().await?;
            }
            Frame::Integers(value) => {
                self.stream.write_u8(b':').await?;
                self.write_number(*value).await?;
                self.put_new_line().await?;
            }
            Frame::Bulk(v) => {
                let len = v.len();
                self.stream.write_u8(b'$').await?;
                self.write_number(len as u64).await?;
                self.stream.write_all(v).await?;
                self.put_new_line().await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Error(err) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(err.as_bytes()).await?;
                self.put_new_line().await?;
            }
            Frame::Array(_) => unreachable!(),
        }
        self.stream.flush().await?;
        Ok(())
    }

    fn parse(&mut self) -> Result<Option<Frame>> {
        let mut buffer = Cursor::new(&self.buffer);
        let len = buffer.position() as usize;
        buffer.set_position(0);
        match Frame::parse(&mut buffer) {
            Ok(frame) => {
                self.buffer.advance(len);
                Ok(Some(frame))
            }
            Err(e) => Err(e),
        }
    }

    async fn put_new_line(&mut self) -> Result<()> {
        Ok(self.stream.write_all(b"\r\n").await?)
    }

    async fn write_number(&mut self, value: u64) -> Result<()> {
        use std::io::Write;
        let mut buffer = [0u8; 12];
        let mut buffer = Cursor::new(&mut buffer[..]);

        write!(&mut buffer, "{}", value)?;

        let pos = buffer.position() as usize;
        //
        self.stream.write_all(&buffer.get_ref()[..pos]).await?;
        self.put_new_line().await?;

        Ok(())
    }
}