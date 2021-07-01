use std::collections::HashMap;

pub struct KvStore {
    map: HashMap<String, String>,
}
impl KvStore {
    pub fn new() -> KvStore {
        let map = HashMap::new();
        KvStore { map }
    }
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }
    pub fn get(&self, key: String) -> Option<String> {
        match self.map.get(&key) {
            Some(v) => Some(v.to_owned()),
            None => None,
        }
    }
    pub fn remove(&mut self, key: String) -> Option<String> {
        self.map.remove(&key)
    }
}
