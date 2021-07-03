mod error;
pub mod log;
pub use error::{Error, ErrorKind, Result};
use log::Logger;
use std::{collections::HashMap, path::Path};

/// Used to store key and value
/// # Example
///
/// ```
/// let mut kvs = KvStore::new();
/// kvs.set("key".to_string(), "value".to_string());
/// assert_eq!(kvs.get("key".to_string()),"value".to_string());
/// ```
pub struct KvStore {
    map: HashMap<String, String>,
    logger: Logger,
}

impl KvStore {
    /// new a key-value store
    /// ```
    /// ```
    pub fn open(path: &Path) -> Result<KvStore> {
        let path = path.join("kvs.db");

        let map = HashMap::new();
        let logger = Logger::new(path)?;

        Ok(KvStore { map, logger })
    }
    /// set the value of a given key
    /// ```
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key.to_owned(), value.to_owned());
        self.logger.log_set(&key, &value)?;
        Ok(())
    }
    /// set the value of a given key
    /// ```
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(v) = self.map.get(&key) {
            return Ok(Some(v.to_owned()));
        }
        match self.logger.get_value(&key) {
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

        match self.logger.get_value(&key) {
            Ok(res) => match res {
                None => Err(Error::from(ErrorKind::KeyNotExist(format!(
                    "key {} you want to remove does not exist",
                    key
                )))),
                Some(value) => {
                    self.logger.log_rem(&key)?;
                    Ok(value)
                }
            },
            Err(e) => Err(e),
        }
    }
}
#[derive(Debug)]
pub enum Command {
    Set,
    Rem,
}
