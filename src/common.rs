use crate::error::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
    Get { key: String },
}

pub trait KvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn remove(&mut self, key: String) -> Result<String>;
}

pub trait DataBase {
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn remove(&mut self, key: String) -> Result<()>;
}
#[derive(Debug)]
pub struct OffSet {
    file_no: u64,
    start: u64,
    len: u64,
}

impl Clone for OffSet {
    fn clone(&self) -> Self {
        OffSet {
            start: self.start,
            len: self.len,
            file_no: self.file_no,
        }
    }
}

impl OffSet {
    pub fn new(file_no: u64, start: u64, end: u64) -> OffSet {
        OffSet {
            file_no,
            start,
            len: end - start,
        }
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn no(&self) -> u64 {
        self.file_no
    }
}
