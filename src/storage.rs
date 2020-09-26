use std::path::PathBuf;
use crate::{Result};
use std::{fs, mem};
use std::fs::{File, OpenOptions};
use walkdir::{WalkDir};
use std::collections::HashMap;
use std::io::{SeekFrom, Seek, Read, Write};
use crate::index::Index;

#[derive(Debug)]
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

pub struct LevelStorage{
    f_readers: HashMap<PathBuf, File>,

    f_appender: File,
    pub append_f_path: PathBuf,
}

impl LevelStorage {
    // pub const LEVEL_F_NUM: u32 = 8;
    // pub const LEVEL_NUM: u32 = 4;
    // // max file size 4k at level 0, 8k at level 1...
    // pub const LEVEL_ZERO_SIZE: u32 = 1024 * 4;
    // pub const FACTOR: u32 = 2;

    pub fn new(path: PathBuf) -> Result<LevelStorage> {
        let mut storage_path = path.clone();
        storage_path.push("data");

        if !storage_path.exists() {
            fs::create_dir(&storage_path)?;
        }

        let append_f_path = LevelStorage::get_path_of_level(&storage_path, 0)?;
        let f_appender = OpenOptions::new().append(true).open(&append_f_path)?;

        Ok(LevelStorage {
            f_readers: HashMap::new(),
            f_appender,
            append_f_path,
        })
    }


    pub fn get_path_of_level(path: &PathBuf, level: u32) -> Result<PathBuf> {
        let mut path_level = path.clone();

        path_level.push(level.to_string());

        if !path_level.exists() {
            fs::create_dir(&path_level)?;
        }

        match WalkDir::new(&path_level).min_depth(1).into_iter().last() {
            Some(res_d) => Ok(res_d.unwrap().into_path()),
            None => {
                path_level.push("kv.data");
                path_level.set_extension(format!("{:08}", 0));
                File::create(&path_level)?;

                Ok(path_level)
            }
        }
    }

    pub fn get(&mut self, lp: &LogPointer) -> Result<String>{
        let f_read = self.f_readers
            .entry(lp.path.clone())
            .or_insert_with(|| {
                File::open(&lp.path).unwrap()
            });

        f_read.seek(SeekFrom::Start(lp.start_pos + mem::size_of::<usize>() as u64))?;

        let mut buf: Vec<u8> = vec![0; lp.record_size];

        f_read.read_exact(&mut buf)?;

        let serialized = String::from_utf8(buf)?;
        Ok(serialized)
    }

    pub fn update(&mut self, serialized: String) -> Result<LogPointer> {
        let record_size = serialized.as_bytes().len();
        let size_buf = record_size.to_be_bytes();

        // let's just use self.path.metadata() here, not quite sure the best way to do
        // this is not good because 1) the system overhead might be large 2) the metadata might not be updated since write is buffered not flushed
        // 3) consitency issue
        // another way I could think of is to keep a u64 of file size in KvStore, this is not good either, like rebuilding the wheel
        let log_pointer = LogPointer::new(
            self.append_f_path.metadata().unwrap().len(), record_size,
            self.append_f_path.clone());

        self.f_appender.write_all(&size_buf)?;
        self.f_appender.write_all(serialized.as_bytes())?;
        self.f_appender.flush()?;

        if self.f_appender.metadata().unwrap().len() > 1024 * 256 {
            self.compaction_after_load()?;
        }

        Ok(log_pointer)
    }

    pub fn compaction_after_load(&mut self) -> Result<()> {
        // new file no need to compaction
        if self.append_f_path.metadata().unwrap().len() == 0 {
            return Ok(());
        }

        let v = self.append_f_path.extension().unwrap()
            .to_str().unwrap()
            .parse::<u32>().unwrap();

        let mut index = Index::new();
        index.initialize(&self.append_f_path)?;

        self.append_f_path.set_extension(format!("{:08}", v + 1));
        File::create(&self.append_f_path)?;

        self.f_appender = OpenOptions::new().append(true).open(&self.append_f_path).unwrap();

        for (_, (_, v)) in index.into_iter().enumerate() {
            let serialized = self.get(&v).unwrap();

            // writing to new appender
            self.update(serialized)?;
        }

        for path_r in self.f_readers.keys() {
            if !path_r.eq(&self.append_f_path) {
                fs::remove_file(path_r)?;
            }
        }

        self.f_readers.clear();
        Ok(())
    }
}