use std::path::PathBuf;
use crate::{Result, KvError};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use failure::_core::cmp::Ordering;
use crate::storage::Storage;
use crate::Index;

pub struct KvStore {
    path: PathBuf,
    storage: Storage,
    index: Index,
}

impl KvStore {

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let storage_path = path.into();
        let mut storage = Storage::new(&storage_path)?;

        let mut index = Index::new();
        storage.build_index(&mut index)?;

        Ok(KvStore {
            path: storage_path.clone(),
            storage,
            index
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {key, value, sequencer: Sequencer::new()?};

        let log_pointer = self.storage.mutate(cmd.clone())?;
        self.index.update_index(&cmd, log_pointer)?;

        if self.storage.should_compaction() {
            self.storage.compaction(&mut self.index)?;
        }
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(lp) = self.index.get_index(&key) {
            let cmd = self.storage.get(&lp)?;
            match cmd.get_value() {
                Some(v) => Ok(Some(v.clone())),
                None => Err(KvError::KeyNotFound),
            }
        } else {
            Ok(None)
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(_) = self.index.get_index(&key) {
            let cmd = Command::Rm {key, sequencer: Sequencer::new()?};
            let log_pointer = self.storage.mutate(cmd.clone())?;

            self.index.update_index(&cmd, log_pointer)?;

            if self.storage.should_compaction() {
                self.storage.compaction(&mut self.index)?;
            }
            Ok(())
        } else {
            Err(KvError::KeyNotFound)
        }
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
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
            }
        )
    }
}

impl PartialOrd for Sequencer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.timestamp.cmp(&other.timestamp))
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum Command {
    Set{key: String, value: String, sequencer: Sequencer},
    Rm{key: String, sequencer: Sequencer},
}

impl Command {
    pub fn get_key(&self) -> &String {
        match self {
            Command::Set {key,..} => key,
            Command::Rm {key,..} => key
        }
    }

    pub fn get_value(&self) -> Option<&String> {
        match self {
            Command::Set {key:_, value: v, sequencer: _} => Some(v),
            Command::Rm {..} => None
        }
    }

    pub fn get_sequencer(&self) -> &Sequencer {
        match self {
            Command::Set {key: _k, value: _v, sequencer: seq} => seq,
            Command::Rm {key: _k, sequencer: seq} => seq,
        }
    }
}