use crate::{LogPointer, Result};
use std::collections::BTreeMap;

pub struct Index {
    kv_index: BTreeMap<String, LogPointer>
}

impl Index {

    pub fn update_index(&mut self, key: String, lp: LogPointer) -> Result<()> {
        Ok(())
    }
}