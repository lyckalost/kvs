use std::collections::HashMap;
use std::path::PathBuf;
use std::fs::{File};
use crate::{KvError};
use std::io::Read;

pub struct Index {
    data_index: HashMap<String, LogPointer>
}

pub struct LogPointer {
    pub start_pos: u64,
    pub record_size: usize,
    pub path: PathBuf
}

impl LogPointer {
    pub fn new(start_pos: u64, record_size: usize, path: PathBuf) -> LogPointer {
        LogPointer {
            start_pos,
            record_size,
            path
        }
    }
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

            f.read_exact(&mut record_size_buf)?;
            let record_size = usize::from_be_bytes(record_size_buf);

            let mut record_buf : Vec<u8> = vec![0; record_size];
            f.read_exact(&mut record_buf)?;
        }
    }

    pub fn update_index(&mut self, key: String, log_pointer: LogPointer) {
        self.data_index.insert(key, log_pointer);
    }

    pub fn get_log_pointer(&self, key: String) -> Option<&LogPointer>{
        self.data_index.get(&key)
    }
}