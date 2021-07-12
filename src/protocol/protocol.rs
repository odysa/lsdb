/// This protocol implements standard of redis protocol
/// Check it out! ---> https://redis.io/topics/protocol
use crate::error::{Error, ErrorKind, Result};
use atoi::atoi;
use bytes::{Buf, Bytes, BytesMut};
use std::convert::TryInto;
use std::io::Cursor;
use std::usize;

pub enum Frame {
    Simple(String),
    Integers(u64),
    Bulk(Bytes),
    Array(Vec<Frame>),
    Null,
    Error(String),
}

impl Frame {
    /*
     * For Simple Strings the first byte of the reply is "+"
     * For Errors the first byte of the reply is "-"
     * For Integers the first byte of the reply is ":"
     * For Bulk Strings the first byte of the reply is "$"
     * For Arrays the first byte of the reply is "*"
     */
    pub fn parse(msg: &mut Cursor<&BytesMut>) -> Result<Frame> {
        match get_u8(msg)? {
            b'+' => {
                let line = get_line(msg)?;
                let result = String::from_utf8(line.to_vec())?;
                Ok(Frame::Simple(result))
            }
            b':' => {
                let result = get_number(msg)?;
                Ok(Frame::Integers(result))
            }
            b'-' => {
                let line = get_line(msg)?;
                let err = String::from_utf8(line.to_vec())?;
                Ok(Frame::Error(err))
            }
            b'*' => {
                let length = get_number(msg)?.try_into()?;
                let mut result = Vec::with_capacity(length);
                for _ in 0..length {
                    result.push(Frame::parse(msg)?);
                }

                Ok(Frame::Array(result))
            }
            _ => Err(Error::from(ErrorKind::InvalidFormat(
                "parsed failed; invalid frame format".to_string(),
            ))),
        }
    }
}

// Gets an unsigned 8 bit integer from cursor of message.
// The current position is advanced by 1
fn get_u8(msg: &mut Cursor<&BytesMut>) -> Result<u8> {
    if !msg.has_remaining() {
        return Err(Error::from(ErrorKind::Incomplete(
            "incomplete frame".to_string(),
        )));
    }

    Ok(msg.get_u8())
}

fn peek_u8(msg: &mut Cursor<&BytesMut>) -> Result<u8> {
    if !msg.has_remaining() {
        return Err(Error::from(ErrorKind::Incomplete(
            "incomplete frame".to_string(),
        )));
    }

    Ok(msg.get_ref()[0])
}

fn get_line<'a>(msg: &mut Cursor<&'a BytesMut>) -> Result<&'a [u8]> {
    let begin = msg.position() as usize;
    let end = msg.get_ref().len() - 1;
    // read util \r\n
    for i in begin..end {
        if check_new_line(&msg.get_ref()[i..i + 2]) {
            msg.set_position((i + 2) as u64);
            return Ok(&msg.get_ref()[begin..i]);
        }
    }
    Err(Error::from(ErrorKind::Incomplete(
        "invalid frame".to_string(),
    )))
}

fn check_new_line(msg: &[u8]) -> bool {
    msg[0] == b'\r' && msg[1] == b'\n'
}

fn get_number(msg: &mut Cursor<&BytesMut>) -> Result<u64> {
    let line = get_line(msg)?;
    atoi::<u64>(line).ok_or_else(|| {
        Error::from(ErrorKind::InvalidFormat(format!(
            "invalid format {:?}",
            line
        )))
    })
}
