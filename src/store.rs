use std::path::PathBuf;
use crate::Result;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

pub struct KvStore {
    path: PathBuf
}

impl KvStore {

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        Ok(KvStore {
            path: path.into()
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(None)
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Sequencer {
    timestamp: u128
}

impl Sequencer {
    // how should I handle this time error = =
    // further more, what should I return if I got this. 500?
    pub fn new() -> Result<Sequencer> {
        Ok(
            Sequencer {
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
            }
        )
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum Command {
    Set{key: String, value: String, sequencer: Sequencer},
    Rm{key: String, sequencer: Sequencer},
}