use crate::{LogPointer, Result, Command, Sequencer, KvError};
use std::collections::BTreeMap;
use std::collections::btree_map::{IterMut};

#[derive(Debug)]
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

        if let Some((_, seq)) = self.kv_index.get(cmd.get_key()) {
            // writing a new sequencer
            if seq.ge(cmd.get_sequencer()) {
                // got conflict
                return Err(KvError::ConflictError)
            }
        }

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

impl<'a> IntoIterator for &'a mut Index {
    // not quite sure what is the lifetime of values if I modify the content
    type Item = (&'a String, &'a mut (LogPointer, Sequencer));
    type IntoIter = IterMut<'a, String, (LogPointer, Sequencer)>;

    fn into_iter(self) -> Self::IntoIter {
        (&mut self.kv_index).iter_mut()
    }
}