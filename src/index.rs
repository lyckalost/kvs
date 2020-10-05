use crate::{LogPointer, Result, Command, Sequencer, KvError};
use std::collections::BTreeMap;

pub struct Index {
    kv_index: BTreeMap<String, (LogPointer, Sequencer)>
}

impl Index {

    pub fn new() -> Self {
        Index {
            kv_index: BTreeMap::new()
        }
    }

    pub fn update_index(&mut self, cmd: &Command, lp: LogPointer) -> Result<()> {
        let mut should_update = false;

        if let Some((_, seq)) = self.kv_index.get(cmd.get_key()) {
            // writing a new sequencer
            if seq.lt(cmd.get_sequencer()) {
                should_update = true;
            } else {
                // got conflict
                return Err(KvError::ConflictError)
            }
        } else { should_update = true; }

        match cmd {
            Command::Rm {..} => {
                self.kv_index.remove(cmd.get_key());
            },
            Command::Set {..} => {
                self.kv_index.insert(cmd.get_key().clone(),
                                     (lp, cmd.get_sequencer().clone()));
            }
        }

        Ok(())
    }

    pub fn get_index(&mut self, key: &String) -> Option<LogPointer> {
        self.kv_index.get(key).and_then(|(lp, _)| Some(lp.clone()))
    }
}