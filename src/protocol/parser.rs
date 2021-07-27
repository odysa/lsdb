use std::{str, vec};

use super::frame::Frame;
use crate::error::{Error, Result};
use atoi::atoi;
use bytes::Bytes;

/// Parse a frame to command
pub struct Parser {
    contents: vec::IntoIter<Frame>,
}

impl Parser {
    pub fn new(&self, frame: Frame) -> Result<Self> {
        let array = match frame {
            Frame::Array(array) => array,
            _ => return Err(Error::from("invalid frame".to_string())),
        };

        let contents = array.into_iter();

        Ok(Parser { contents })
    }

    pub fn next(&mut self) -> Result<Frame> {
        self.contents
            .next()
            .ok_or(Error::from("frame empty".to_string()))
    }

    pub fn next_string(&mut self) -> Result<String> {
        match self.next()? {
            Frame::Simple(v) => Ok(v),
            Frame::Bulk(v) => Ok(str::from_utf8(&v)?.to_string()),
            _ => Err(Error::from("invalid frame".to_string())),
        }
    }

    pub fn next_int(&mut self) -> Result<u64> {
        match self.next()? {
            Frame::Simple(v) => {
                atoi(&v.as_bytes()).ok_or(Error::from(format!("cannot convert {} to int", v)))
            }
            Frame::Integers(v) => Ok(v),
            Frame::Bulk(v) => atoi(&v).ok_or(Error::from(format!("cannot convert {:?} to int", v))),
            _ => Err(Error::from("invalid frame".to_string())),
        }
    }

    pub fn next_bytes(&mut self) -> Result<Bytes> {
        match self.next()? {
            Frame::Simple(v) => Ok(Bytes::from(v.into_bytes())),
            Frame::Bulk(v) => Ok(v),
            _ => Err(Error::from("invalid frame".to_string())),
        }
    }
}
