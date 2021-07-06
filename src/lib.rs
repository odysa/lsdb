mod error;
pub mod log;
mod log_reader;
mod log_writer;
pub use error::{Error, ErrorKind, Result};
use log::Logger;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::Path};

/// Used to store key and value
/// # Example
///
/// ```
/// use kvs::KvStore;
/// use std::path::Path;
///
/// let path = Path::new("");
/// let mut kvs = KvStore::open(path).unwrap();
/// kvs.set("key".to_string(), "value".to_string()).unwrap();
/// assert_eq!(kvs.get("key".to_string()).unwrap(),Some("value".to_string()));
/// ```
pub struct KvStore<T: DataMaintainer> {
    map: HashMap<String, String>,
    maintainer: T,
}

impl KvStore<Logger> {
    pub fn open(path: &Path) -> Result<Self> {
        let path = path.join("kvs.db");
        let maintainer = Logger::new(path)?;
        Ok(KvStore {
            maintainer,
            map: HashMap::new(),
        })
    }
}

impl<T: DataMaintainer> KvStore<T> {
    /// new a key-value store
    /// ```
    /// ```

    /// set the value of a given key
    /// ```
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key.to_owned(), value.to_owned());
        self.maintainer.set(&key, &value)?;
        Ok(())
    }
    /// set the value of a given key
    /// ```
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(v) = self.map.get(&key) {
            return Ok(Some(v.to_owned()));
        }
        match self.maintainer.get(&key) {
            Ok(res) => {
                if let Some(value) = res {
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e),
        }
    }
    /// remove a given key in store
    /// ```
    /// ```
    pub fn remove(&mut self, key: String) -> Result<String> {
        self.map.remove(&key).unwrap_or_default();

        match self.maintainer.get(&key) {
            Ok(res) => match res {
                None => Err(Error::from(ErrorKind::KeyNotExist(format!(
                    "key {} you want to remove does not exist",
                    key
                )))),
                Some(value) => {
                    self.maintainer.rem(&key)?;
                    Ok(value)
                }
            },
            Err(e) => Err(e),
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Rem { key: String },
}

pub trait DataMaintainer {
    fn get(&mut self, key: &str) -> Result<Option<String>>;
    fn set(&mut self, key: &str, value: &str) -> Result<()>;
    fn rem(&mut self, key: &str) -> Result<()>;
}
