use crate::{Result, KvError, Index};
use crate::store::{Command, Sequencer};
use std::path::{PathBuf, Path};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, BufReader, SeekFrom, Write, BufWriter};
use std::fmt::Display;
use failure::_core::fmt::Formatter;
use std::ffi::OsStr;
use failure::_core::cmp::Ordering;
use std::collections::{BTreeMap};
use serde_json::Deserializer;

#[derive(Clone, Debug, PartialEq)]
pub struct LogPointer {
    pub start_pos: u64,
    pub len: u64,
    pub f_id: FileId,
}

pub struct Storage {
    storage_path: PathBuf,
    readers: BTreeMap<FileId, BufferedReaderWithPos<File>>,
    writer: BufferedWriterWithPos<File>,
    current_f_id: FileId,
}

impl Storage {
    pub const MAX_LOG_SIZE: u64 = 1024 * 32;

    pub fn new(path: &PathBuf) -> Result<Storage> {
        let mut storage_path = path.clone();
        storage_path.push("data");

        fs::create_dir_all(&storage_path)?;
        let mut readers: BTreeMap<FileId, BufferedReaderWithPos<File>> = BTreeMap::new();
        let sorted_f_id_l = Storage::sorted_f_id_list(&storage_path)?;
        for f_id in &sorted_f_id_l {
            readers.insert(f_id.clone(),
                           BufferedReaderWithPos::new(
                               File::open(Storage::log_path(&f_id, &storage_path))?)?
            );
        }

        let writer_id = sorted_f_id_l.last().unwrap_or(&FileId {id: 0}).inc();
        let writer = Storage::new_log_file(&writer_id, &storage_path, &mut readers)?;

        Ok(Storage {
            storage_path,
            readers,
            writer,
            current_f_id: writer_id,
        })
    }

    pub fn get(&mut self, lp: &LogPointer) -> Result<Command> {
        if let Some(reader) = self.readers.get_mut(&lp.f_id) {
            reader.seek(SeekFrom::Start(lp.start_pos))?;
            // this is reading from reader with a fixed lenth
            // when we do loading at start, it is like reading in a stream way
            let cmd_reader = reader.take(lp.len);

            let cmd: Command = serde_json::from_reader(cmd_reader)?;
            Ok(cmd)
        } else {
            Err(KvError::KeyNotFound)
        }

    }

    pub fn build_index(&mut self, index: &mut Index) -> Result<()> {
        for (f_id, reader) in self.readers.iter_mut() {
            let mut pos = reader.seek(SeekFrom::Start(0))?;

            // WTF! How can this reader ref get into that Deserializer::from_reader
            // not 100% percent sure, but maybe related to how the trait is defined for references
            // https://stackoverflow.com/questions/44928882/why-do-i-get-the-error-the-trait-foo-is-not-implemented-for-mut-t-even-th
            let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
            while let Some(cmd) = stream.next() {
                let new_pos = stream.byte_offset() as u64;
                index.update_index(&cmd?, LogPointer {start_pos: pos, len: new_pos - pos, f_id: f_id.clone()})?;

                pos = new_pos;
            }
        }

        Ok(())
    }

    pub fn mutate(&mut self, cmd: Command) -> Result<LogPointer> {
        let start_pos = self.writer.pos;

        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        let new_pos = self.writer.pos;

        let lp = LogPointer {
            start_pos,
            len: new_pos - start_pos,
            f_id: self.current_f_id.clone()
        };

        if new_pos > Storage::MAX_LOG_SIZE {
            let writer_id = self.current_f_id.inc();
            let writer = Storage::new_log_file(&writer_id, &self.storage_path, &mut self.readers)?;
            self.current_f_id = writer_id;
            self.writer = writer;
        }

        Ok(lp)
    }

    pub fn should_compaction(&mut self) -> bool {
        self.readers.len() > 4
    }

    pub fn compaction(&mut self, index: &mut Index) -> Result<()> {
        // should make this async later
        let stop_f_id = self.current_f_id.clone();

        let writer_id = self.current_f_id.inc();
        let writer = Storage::new_log_file(&writer_id, &self.storage_path, &mut self.readers)?;
        self.current_f_id = writer_id;
        self.writer = writer;

        for (_, v) in index.into_iter() {
            let lp: &LogPointer = &v.0;
            let seq: &Sequencer = &v.1;
            let cmd = self.get(lp)?;
            let lp_updated = self.mutate(cmd)?;

            // want to get rid of this clone, but seems not possible
            *v = (lp_updated, seq.clone());
        }


        // clean up old files
        let sorted_f_id_l = Storage::sorted_f_id_list(&self.storage_path)?;
        for f_id in sorted_f_id_l {
            // new files, ignore
            if f_id.gt(&stop_f_id) {
                continue;
            }

            fs::remove_file(Storage::log_path(&f_id, &self.storage_path))?;
        }
        Ok(())
    }

    fn sorted_f_id_list(path: &Path) -> Result<Vec<FileId>> {
        let mut f_id_list: Vec<FileId> = fs::read_dir(&path)?
            .flat_map(|res| -> Result<_> { Ok(res?.path()) })
            .filter(|path| path.is_file() && path.extension() == Some("dat".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .map(|s| s.trim_end_matches(".dat"))
                    .map(|s| s.parse::<u64>().and_then(|id| Ok(FileId {id})))
            })
            .flatten()
            .collect();

        f_id_list.sort_unstable();
        Ok(f_id_list)
    }

    fn log_path(f_id: &FileId, path: &Path) -> PathBuf {
        path.join(format!("{}.dat", f_id))
    }

    fn new_log_file(f_id: &FileId, path: &Path,
                    readers: &mut BTreeMap<FileId, BufferedReaderWithPos<File>>) -> Result<BufferedWriterWithPos<File>> {
        let new_path = Storage::log_path(f_id, path);

        let writer = BufferedWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&new_path)?
        )?;

        readers.insert(f_id.clone(), BufferedReaderWithPos::new(File::open(&new_path)?)?);

        Ok(writer)
    }
}

struct BufferedReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufferedReaderWithPos<R> {
    pub fn new(mut inner: R) -> io::Result<Self> {
        inner.seek(SeekFrom::Start(0))?;

        Ok(BufferedReaderWithPos {
            reader: BufReader::new(inner),
            pos: 0,
        })
    }
}

impl<R: Read + Seek> Read for BufferedReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;

        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufferedReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = self.reader.seek(pos)?;
        Ok(new_pos)
    }
}

struct BufferedWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufferedWriterWithPos<W> {
    pub fn new(mut inner: W) -> io::Result<Self> {
        inner.seek(SeekFrom::Start(0))?;

        Ok(
            BufferedWriterWithPos {
                writer: BufWriter::new(inner),
                pos: 0,
            }
        )
    }
}

impl<W: Write + Seek> Write for BufferedWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;

        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufferedWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = self.writer.seek(pos)?;

        Ok(new_pos)
    }
}

#[derive(Eq, PartialEq, PartialOrd, Hash, Clone, Debug)]
pub struct FileId {
    pub id: u64
}

impl FileId {
    pub fn inc(&self) -> FileId {
        FileId {
            id: self.id + 1
        }
    }
}

impl Display for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:08}", self.id)
    }
}

impl Ord for FileId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}