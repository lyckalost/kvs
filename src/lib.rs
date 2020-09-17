use std::collections::HashMap;

pub struct KvStore {
    store: HashMap<String, String>
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore{
            store: HashMap::new()
        }
    }

    // get is get(&K, &V) just in case of ownership transfer
    pub fn get(&self, key: String) -> Option<String> {
        match self.store.get(&key) {
            Some(s) => Some(String::clone(s)),
            None => None,
        }
    }

    // for set, K,V ownership is transferred to store
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    // &K just in case of ownership transfer
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
