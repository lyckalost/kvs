mod storage;
mod index;

use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use crate::index::{LogPointer, Index};
use std::fs::{File, OpenOptions};
use std::io::{Write, Seek, SeekFrom, Read};
use serde_json;
use std::mem;

pub struct KvStore {
    index: Index,
    path: PathBuf,
    f_append : File,
    f_read: File,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Command {
    op: String,
    k: String,
    v: String,
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
        let path_buf = path.into();
        let f_append = OpenOptions::new().append(true).open(path_buf.clone())?;
        let f_read = OpenOptions::new().read(true).open(path_buf.clone())?;

        let mut index = Index::new();
        index.initialize(&path_buf)?;

        Ok(KvStore {
            index,
            path: path_buf.clone(),
            f_append,
            f_read,
        })
    }
}

impl KvStore {
    // get is get(&K, &V) just in case of ownership transfer
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // match self.store.get(&key) {
        //     Some(s) => Ok(Some(String::clone(s))),
        //     None => Ok(None),
        // }
        match self.index.get_log_pointer(key) {
            None => Ok(None),
            Some(lp) => {
                // start pos shift by record size padding
                self.f_read.seek(SeekFrom::Start(lp.start_pos + mem::size_of::<usize>() as u64))?;
                let mut buf: Vec<u8> = vec![0; lp.record_size];

                self.f_read.read_exact(&mut buf)?;

                let serialized = String::from_utf8(buf)?;
                Ok(serde_json::from_str(&serialized)?)
            }
        }
    }

    // for set, K,V ownership is transferred to store
    pub fn set(&mut self, key: String, value: String) -> Result<()>{
        let serialized = serde_json::to_string(&Command {op: "set".to_owned(), k: key.clone(), v: value})?;
        let record_size = serialized.as_bytes().len();

        let size_buf = record_size.to_be_bytes();

        // let's just use self.path.metadata() here, not quite sure the best way to do
        // this is not good because 1) the system overhead might be large 2) the metadata might not be updated since write is buffered not flushed
        // 3) consitency issue
        // another way I could think of is to keep a u64 of file size in KvStore, this is not good either, like rebuilding the wheel
        self.index.update_index(key,
                                LogPointer::new(self.path.metadata().unwrap().len(), record_size, self.path.clone())
        );

        // self.index.update_index(key.clone(), self.path.clone(),
        //                         self.path.metadata().unwrap().len(), record_size);

        self.f_append.write_all(&size_buf)?;
        self.f_append.write_all(serialized.as_bytes())?;

        Ok(())
    }

    // &K just in case of ownership transfer
    pub fn remove(&mut self, key: String) -> Result<()> {
        let serialized = serde_json::to_string(&Command {op: "rm".to_owned(), k: key.clone(), v: String::new()})?;
        let record_size = serialized.as_bytes().len();

        let size_buf = record_size.to_be_bytes();

        self.index.update_index(key,
                                LogPointer::new(self.path.metadata().unwrap().len(), record_size, self.path.clone())
        );

        self.f_append.write_all(&size_buf)?;
        self.f_append.write_all(serialized.as_bytes())?;

        Ok(())
    }
}
