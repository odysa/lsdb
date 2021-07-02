mod error;
pub mod log;
pub use error::{Error, Result};
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
        self.logger.append(format!("set,{},{}", key, value))?;
        Ok(())
    }
    /// set the value of a given key
    /// ```
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(match self.map.get(&key) {
            Some(v) => {
                self.logger.append(format!("get,{}", key))?;
                Some(v.to_owned())
            }
            None => {
                eprintln!("key not found");
                None
            }
        })
    }
    /// remove a given key in store
    /// ```
    /// ```
    pub fn remove(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.map.remove(&key))
    }
}
