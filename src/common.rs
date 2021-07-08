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
    fn remove(&mut self, key: String) -> Result<()>;
}

pub struct OffSet {
    start: u64,
    len: u64,
    value: Option<String>,
}

impl Clone for OffSet {
    fn clone(&self) -> Self {
        OffSet {
            start: self.start,
            len: self.len,
            value: self.value.to_owned(),
        }
    }
}

impl OffSet {
    pub fn new(start: u64, end: u64, value: Option<String>) -> OffSet {
        OffSet {
            start,
            len: end - start,
            value,
        }
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn value(&self) -> Option<String> {
        self.value.to_owned()
    }
}
