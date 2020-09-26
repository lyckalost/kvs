mod storage;
mod index;

use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use crate::index::Index;
use serde_json;
use crate::storage::{LevelStorage};

pub struct KvStore {
    index: Index,
    storage: LevelStorage,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Sequencer {
    timestamp: u128
}

impl Sequencer {

    pub fn new(timestamp: u128) -> Sequencer {
        Sequencer {
            timestamp
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Command {
    pub op: String,
    pub k: String,
    pub v: String,
    pub sequencer: Sequencer,
}

impl Command {
    pub fn new(op: String, k: String, v: String, sequencer: Sequencer) -> Command {
        Command {
            op,
            k,
            v,
            sequencer,
        }
    }
}

// define our own error type
#[derive(Debug, Clone)]
pub struct KvError {
    details: String
}

impl From<std::io::Error> for KvError {
    fn from(error: std::io::Error) -> Self {
        KvError {
            details: error.to_string()
        }
    }
}

impl From<serde_json::error::Error> for KvError {
    fn from(error: serde_json::error::Error) -> Self {
        KvError {
            details: error.to_string()
        }
    }
}

impl From<std::string::FromUtf8Error> for KvError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        KvError {
            details: error.to_string()
        }
    }
}

pub type Result<T> = std::result::Result<T, KvError>;

impl KvStore {

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let work_path = path.into();

        let mut storage = LevelStorage::new(work_path.clone())?;
        storage.compaction_after_load()?;

        let mut index = Index::new();
        index.initialize(&storage.append_f_path)?;


        Ok(KvStore {
            index,
            storage,
        })
    }
}

impl KvStore {
    // get is get(&K, &V) just in case of ownership transfer
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get_log_pointer(&key) {
            None => Ok(None),
            Some(lp) => {
                let serialized = self.storage.get(lp)?;
                let deserialized: Command = serde_json::from_str(&serialized)?;
                Ok(Some(deserialized.v))
            }
        }
    }

    // for set, K,V ownership is transferred to store
    pub fn set(&mut self, key: String, value: String) -> Result<()>{
        let serialized = serde_json::to_string(&Command {op: "set".to_owned(), k: key.clone(), v: value, sequencer: Sequencer::new(0)})?;

        let log_pointer = self.storage.update(serialized)?;
        self.index.update_index(key,log_pointer);

        Ok(())
    }

    // &K just in case of ownership transfer
    pub fn remove(&mut self, key: String) -> Result<()> {
        // if key does not exist do nothing
        match self.index.get_log_pointer(&key) {
            None => {
                println!("Key not found");
                Err(KvError { details: "Key not found".to_owned() })
            },
            Some(_) => {
                let serialized = serde_json::to_string(&Command {op: "rm".to_owned(), k: key.clone(), v: String::new(), sequencer : Sequencer::new(0)})?;
                self.storage.update(serialized)?;
                self.index.delete_index(&key);

                Ok(())
            }
        }
    }
}
