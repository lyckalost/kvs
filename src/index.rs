use std::collections::HashMap;
use std::path::PathBuf;
use std::fs::{File};
use crate::{KvError, Command};
use std::io::Read;
use crate::storage::LogPointer;
use std::collections::hash_map::IntoIter;

#[derive(Debug)]
pub struct Index {
    data_index: HashMap<String, LogPointer>
}

impl Index {
    pub fn new() -> Index {
        Index {
            data_index: HashMap::new()
        }
    }

    pub fn initialize(&mut self, pathbuf: &PathBuf) -> crate::Result<()>{
        let mut f = File::open(pathbuf)?;
        let mut record_size_buf = (0 as usize).to_be_bytes();

        let mut total_bytes_now: u64 = 0;

        // here we tries to borrow from read_exact
        // if we can't read any number we consider this as done
        loop {
            let mut tmp: &mut[u8] = &mut record_size_buf;
            while !tmp.is_empty() {
                match f.read(tmp) {
                    Ok(0) => { return Ok(()); },
                    Ok(n) => {
                        tmp = &mut tmp[n..];
                    },
                    Err(e) => {
                        return Err(KvError {details: e.to_string()})
                    },
                }
            }
            if !tmp.is_empty() {
                return Err(KvError {details : "error setting up index".to_owned()});
            }

            // f.read_exact(&mut record_size_buf)?;
            let record_size = usize::from_be_bytes(record_size_buf);

            let mut record_buf : Vec<u8> = vec![0; record_size];
            f.read_exact(&mut record_buf)?;

            let serialized = String::from_utf8(record_buf)?;
            let deserialized: Command = serde_json::from_str(&serialized)?;
            match &deserialized.op[..] {
                "set" => { self.update_index(deserialized.k,
                                             LogPointer::new(total_bytes_now, record_size, pathbuf.clone()));}
                "rm" => { self.delete_index(&deserialized.k); }
                _ =>  { return Err(KvError { details: "Unknown operation in log".to_owned() }); }
            }
            total_bytes_now += (record_size + record_size_buf.len()) as u64;
        }
    }

    pub fn update_index(&mut self, key: String, log_pointer: LogPointer) {
        self.data_index.insert(key, log_pointer);
    }

    pub fn delete_index(&mut self, key: &String) {
        self.data_index.remove(key);
    }

    pub fn get_log_pointer(&self, key: &String) -> Option<&LogPointer>{
        self.data_index.get(key)
    }
}

impl IntoIterator for Index {
    type Item = (String, LogPointer);
    type IntoIter = IntoIter<String, LogPointer>;

    fn into_iter(self) -> Self::IntoIter {
        self.data_index.into_iter()
    }
}