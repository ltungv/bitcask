//! An implementation of [Bitcask]. This is essentially similar to [`engine::LogStructuredHashTable`],
//! but here we closely follow the design described in the paper and try to provide a similar set of APIs.
//!
//! [Bitcask]: (https://riak.com/assets/bitcask-intro.pdf).
//! [`engine::LogStructuredHashTable`]: opal::engine::LogStructuredHashTable

pub(crate) mod bufio;
pub(crate) mod logdir;
pub(crate) mod logfile;
pub(crate) mod utils;

use std::{
    cell::RefCell,
    fs,
    io::{self, Seek, Write},
    path::{self, Path},
    sync::{atomic, Arc, Mutex},
};

use crossbeam::{queue::ArrayQueue, utils::Backoff};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::debug;

use self::logfile::LogReader;

use super::KeyValueStore;
use logdir::LogDir;
use logfile::{LogIndex, LogWriter};

/// A wrapper around [`BitCask`] that implements the `KeyValueStore` trait.
#[derive(Clone, Debug)]
pub struct BitCaskKeyValueStore(pub BitCask);

impl KeyValueStore for BitCaskKeyValueStore {
    type Error = BitCaskError;

    fn set(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.0.put(key, value)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.0.get(key)
    }

    fn del(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.0.delete(key)
    }
}

/// Error returned by this module
#[derive(Error, Debug)]
pub enum BitCaskError {
    #[error("Entry `{0:?}` is invalid")]
    BadKeyDirEntry(KeyDirEntry),

    #[error("I/O error - {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error - {0}")]
    Serialization(#[from] bincode::Error),
}

/// Configuration for a `BitCask` instance.
#[derive(Debug, Clone)]
pub struct BitCaskConfig {
    max_datafile_size: u64,
    max_garbage_size: u64,
    concurrency: usize,
}

impl BitCaskConfig {
    /// Create a `BitCask` instance at the given path with the available options.
    pub fn open<P>(&self, path: P) -> Result<BitCask, BitCaskError>
    where
        P: AsRef<Path>,
    {
        BitCask::open(path, self)
    }

    /// Set the max data file size. The writer will open a new data file when reaches this value.
    pub fn max_datafile_size(&mut self, max_datafile_size: u64) -> &mut Self {
        self.max_datafile_size = max_datafile_size;
        self
    }

    /// Set the max garbage size. The writer will merge data files when reaches this value.
    pub fn max_garbage_size(&mut self, max_garbage_size: u64) -> &mut Self {
        self.max_garbage_size = max_garbage_size;
        self
    }

    /// Set the max number of concurrent readers.
    pub fn concurrency(&mut self, concurrency: usize) -> &mut Self {
        self.concurrency = concurrency;
        self
    }
}

impl Default for BitCaskConfig {
    fn default() -> Self {
        Self {
            max_datafile_size: 2 * 1024 * 1024 * 1024, // 2 GBs
            max_garbage_size: 2 * 1024 * 1024 * 1024 * 30 / 100, // 30% of 2 GBs
            concurrency: num_cpus::get_physical(),
        }
    }
}

/// Implementation of Bitcask.
///
/// Each Bitcask instance is a directory containing data files. At any moment, one file is "active"
/// for writing, and Bitcask ensures that writes are sequentially consistent. Data files are
/// append-only, and Bitcask keeps a "keydir" that maps the key to the position of its value in the
/// data files allowing for random data access.
#[derive(Clone, Debug)]
pub struct BitCask {
    writer: Arc<Mutex<BitCaskWriter>>,
    readers: Arc<ArrayQueue<BitCaskReader>>,
}

impl BitCask {
    fn open<P>(path: P, conf: &BitCaskConfig) -> Result<BitCask, BitCaskError>
    where
        P: AsRef<Path>,
    {
        let (keydir, active_fileid, garbage) = rebuild_keydir(&path)?;
        debug!(?active_fileid, "Got new activate file ID");

        let ctx = Arc::new(BitCaskContext {
            conf: conf.clone(),
            path: path.as_ref().to_path_buf(),
            min_fileid: atomic::AtomicU64::new(0),
            keydir,
        });

        let readers = Arc::new(ArrayQueue::new(conf.concurrency));
        for _ in 0..conf.concurrency {
            readers
                .push(BitCaskReader {
                    ctx: Arc::clone(&ctx),
                    readers: Default::default(),
                })
                .expect("unreachable error");
        }

        let writer = LogWriter::create(path.as_ref().join(utils::datafile_name(active_fileid)))?;
        let writer = Arc::new(Mutex::new(BitCaskWriter {
            ctx,
            readers: Default::default(),
            writer,
            active_fileid,
            written: 0,
            garbage,
        }));

        Ok(BitCask { writer, readers })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BitCaskError> {
        let backoff = Backoff::new();
        loop {
            if let Some(reader) = self.readers.pop() {
                // Make a query with the key and return the context to the queue after we finish so
                // other threads can make progress
                let result = reader.get(key);
                self.readers.push(reader).expect("unreachable error");
                break result;
            }
            // Spin until we have access to a read context
            backoff.spin();
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, BitCaskError> {
        self.writer.lock().unwrap().put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BitCaskError> {
        self.writer.lock().unwrap().delete(key)
    }

    fn merge(&self) -> Result<(), BitCaskError> {
        self.writer.lock().unwrap().merge()
    }

    fn sync_all(&self) -> Result<(), BitCaskError> {
        self.writer.lock().unwrap().sync_all()
    }
}

type KeyDir = DashMap<Vec<u8>, KeyDirEntry>;

#[derive(Debug, Clone)]
pub struct KeyDirEntry {
    fileid: u64,
    len: u64,
    pos: u64,
    tstamp: u128,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HintFileEntry {
    pub tstamp: u128,
    pub len: u64,
    pub pos: u64,
    pub key: Vec<u8>,
}

/// The data entry that gets appended to the data file on write.
#[derive(Serialize, Deserialize, Debug)]
pub struct DataFileEntry {
    /// Local unix nano timestamp.
    pub tstamp: u128,
    /// Data of the key.
    pub key: Vec<u8>,
    /// Data of the value.
    pub value: DataFileEntryValue,
}

/// The data file entry's value.
#[derive(Serialize, Deserialize, Debug)]
pub enum DataFileEntryValue {
    /// The key has been deleted.
    Tombstone,
    /// The value of the key.
    Data(Vec<u8>),
}

#[derive(Debug)]
struct BitCaskContext {
    conf: BitCaskConfig,
    path: path::PathBuf,
    min_fileid: atomic::AtomicU64,
    keydir: KeyDir,
}

#[derive(Debug)]
struct BitCaskWriter {
    ctx: Arc<BitCaskContext>,
    readers: RefCell<LogDir>,
    writer: LogWriter,
    active_fileid: u64,
    written: u64,
    garbage: u64,
}

impl BitCaskWriter {
    /// Set the value of a key, overwriting any existing value at that key.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, BitCaskError> {
        let tstamp = utils::timestamp();
        let index = self.put_data(tstamp, key, value)?;

        let keydir_entry = KeyDirEntry {
            fileid: self.active_fileid,
            len: index.len,
            pos: index.pos,
            tstamp,
        };

        match self.ctx.keydir.insert(key.to_vec(), keydir_entry) {
            Some(prev_keydir_entry) => {
                let (_, prev_datafile_entry) = self
                    .readers
                    .borrow_mut()
                    .get(&self.ctx.path, prev_keydir_entry.fileid)?
                    .at::<DataFileEntry>(prev_keydir_entry.pos)?;
                self.collect_garbage(prev_keydir_entry.len)?;
                match prev_datafile_entry.value {
                    DataFileEntryValue::Data(v) => Ok(Some(v)),
                    DataFileEntryValue::Tombstone => {
                        Err(BitCaskError::BadKeyDirEntry(prev_keydir_entry))
                    }
                }
            }
            None => Ok(None),
        }
    }

    /// Delete a key and its value, if it exists.
    ///
    /// # Error
    ///
    /// If the deleted key does not exist, returns an error of kind `ErrorKind::KeyNotFound`.
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn delete(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, BitCaskError> {
        match self.ctx.keydir.remove(key) {
            Some((_, prev_keydir_entry)) => {
                let (_, prev_datafile_entry) = self
                    .readers
                    .borrow_mut()
                    .get(&self.ctx.path, prev_keydir_entry.fileid)?
                    .at::<DataFileEntry>(prev_keydir_entry.pos)?;
                self.collect_garbage(prev_keydir_entry.len)?;
                self.put_tombstone(utils::timestamp(), key)?;
                match prev_datafile_entry.value {
                    DataFileEntryValue::Data(v) => Ok(Some(v)),
                    DataFileEntryValue::Tombstone => {
                        Err(BitCaskError::BadKeyDirEntry(prev_keydir_entry))
                    }
                }
            }
            None => Ok(None),
        }
    }

    fn merge(&mut self) -> Result<(), BitCaskError> {
        let merge_fileid = self.active_fileid + 1;
        let mut merge_datafile_writer =
            LogWriter::create(self.ctx.path.join(utils::datafile_name(merge_fileid)))?;
        let mut merge_hintfile_writer =
            LogWriter::create(self.ctx.path.join(utils::hintfile_name(merge_fileid)))?;

        let mut readers = self.readers.borrow_mut();
        let mut merge_pos = 0;

        for mut keydir_entry in self.ctx.keydir.iter_mut() {
            let reader = readers.get(&self.ctx.path, keydir_entry.fileid)?;
            reader.seek(io::SeekFrom::Start(keydir_entry.pos))?;
            let nbytes = io::copy(reader, &mut merge_datafile_writer)?;

            keydir_entry.fileid = merge_fileid;
            keydir_entry.pos = merge_pos;
            merge_pos += nbytes;

            merge_hintfile_writer.append(&HintFileEntry {
                tstamp: keydir_entry.tstamp,
                len: keydir_entry.len,
                pos: keydir_entry.pos,
                key: keydir_entry.key().clone(),
            })?;
        }

        merge_datafile_writer.flush()?;
        merge_hintfile_writer.flush()?;

        let stale_fileids = readers.stale_fileids(merge_fileid);
        readers.drop_stale(merge_fileid);
        drop(readers);

        for id in stale_fileids {
            let hintfile_path = self.ctx.path.join(utils::hintfile_name(id));
            let datafile_path = self.ctx.path.join(utils::datafile_name(id));

            if hintfile_path.exists() {
                fs::remove_file(hintfile_path)?;
            }
            if datafile_path.exists() {
                fs::remove_file(datafile_path)?;
            }
        }

        self.garbage = 0;
        self.new_active_datafile(self.active_fileid + 2)?;
        Ok(())
    }

    fn put_data(
        &mut self,
        tstamp: u128,
        key: &[u8],
        value: &[u8],
    ) -> Result<LogIndex, BitCaskError> {
        let entry = DataFileEntry {
            tstamp,
            key: key.to_vec(),
            value: DataFileEntryValue::Data(value.to_vec()),
        };
        let index = self.writer.append(&entry)?;
        self.writer.flush()?;
        self.collect_written(index.len)?;
        Ok(index)
    }

    fn put_tombstone(&mut self, tstamp: u128, key: &[u8]) -> Result<(), BitCaskError> {
        let entry = DataFileEntry {
            tstamp,
            key: key.to_vec(),
            value: DataFileEntryValue::Tombstone,
        };
        let index = self.writer.append(&entry)?;
        self.writer.flush()?;
        self.collect_written(index.len)?;
        self.collect_garbage(index.len)?;
        Ok(())
    }

    fn new_active_datafile(&mut self, fileid: u64) -> Result<(), BitCaskError> {
        self.active_fileid = fileid;
        self.writer =
            LogWriter::create(self.ctx.path.join(utils::datafile_name(self.active_fileid)))?;
        Ok(())
    }

    fn collect_written(&mut self, sz: u64) -> Result<(), BitCaskError> {
        self.written += sz;
        if self.written > self.ctx.conf.max_datafile_size {
            self.new_active_datafile(self.active_fileid + 1)?;
        }
        Ok(())
    }

    fn collect_garbage(&mut self, sz: u64) -> Result<(), BitCaskError> {
        self.garbage += sz;
        if self.garbage > self.ctx.conf.max_garbage_size {
            self.merge()?;
        }
        Ok(())
    }

    fn sync_all(&mut self) -> Result<(), BitCaskError> {
        self.writer.sync_all()?;
        Ok(())
    }
}

#[derive(Debug)]
struct BitCaskReader {
    ctx: Arc<BitCaskContext>,
    readers: RefCell<LogDir>,
}

impl BitCaskReader {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BitCaskError> {
        match self.ctx.keydir.get(key) {
            None => Ok(None),
            Some(index) => {
                let min_fileid = self.ctx.min_fileid.load(atomic::Ordering::SeqCst);
                let mut readers = self.readers.borrow_mut();
                readers.drop_stale(min_fileid);

                let keydir_entry = index.value();
                let (_, datafile_entry) = readers
                    .get(&self.ctx.path, keydir_entry.fileid)?
                    .at::<DataFileEntry>(keydir_entry.pos)?;

                match datafile_entry.value {
                    DataFileEntryValue::Data(v) => Ok(Some(v)),
                    DataFileEntryValue::Tombstone => {
                        Err(BitCaskError::BadKeyDirEntry(keydir_entry.clone()))
                    }
                }
            }
        }
    }
}

fn rebuild_keydir<P>(path: P) -> Result<(KeyDir, u64, u64), BitCaskError>
where
    P: AsRef<Path>,
{
    let keydir = KeyDir::default();
    let fileids = utils::sorted_fileids(&path)?;
    let active_fileid = fileids.last().map(|id| id + 1).unwrap_or_default();

    let mut garbage = 0;
    for fileid in fileids {
        let hintfile_path = path.as_ref().join(utils::hintfile_name(fileid));
        if hintfile_path.exists() {
            populate_keydir_with_hintfile(&keydir, fileid, hintfile_path)?;
        } else {
            let datafile_path = path.as_ref().join(utils::datafile_name(fileid));
            garbage += populate_keydir_with_datafile(&keydir, fileid, datafile_path)?;
        }
    }
    Ok((keydir, active_fileid, garbage))
}

fn populate_keydir_with_hintfile<P>(
    keydir: &KeyDir,
    fileid: u64,
    path: P,
) -> Result<(), BitCaskError>
where
    P: AsRef<Path>,
{
    let mut hintfile_reader = LogReader::open(path)?;
    while let Some((_, entry)) = hintfile_reader.next::<HintFileEntry>()? {
        keydir.insert(
            entry.key,
            KeyDirEntry {
                fileid,
                len: entry.len,
                pos: entry.pos,
                tstamp: entry.tstamp,
            },
        );
    }
    Ok(())
}

fn populate_keydir_with_datafile<P>(
    keydir: &KeyDir,
    fileid: u64,
    path: P,
) -> Result<u64, BitCaskError>
where
    P: AsRef<Path>,
{
    let mut garbage = 0;
    let mut datafile_reader = LogReader::open(path)?;
    while let Some((datafile_index, datafile_entry)) = datafile_reader.next::<DataFileEntry>()? {
        match datafile_entry.value {
            DataFileEntryValue::Tombstone => garbage += datafile_index.len,
            DataFileEntryValue::Data(_) => {
                if let Some(prev_keydir_entry) = keydir.insert(
                    datafile_entry.key,
                    KeyDirEntry {
                        fileid,
                        len: datafile_index.len,
                        pos: datafile_index.pos,
                        tstamp: datafile_entry.tstamp,
                    },
                ) {
                    garbage += prev_keydir_entry.len;
                }
            }
        }
    }
    Ok(garbage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::quickcheck;

    quickcheck! {
        fn fileids_sorted_correctly(n: u64) -> bool {
            // keep the file handles until end of scope
            let mut tmps = Vec::new();
            // Create random datafiles and hintfiles in the directory
            let dir = tempfile::tempdir().unwrap();
            for fileid in 0..n.min(100) {
                tmps.push(fs::File::create(dir.path().join(utils::datafile_name(fileid))).unwrap());
                if rand::random() {
                    tmps.push(fs::File::create(dir.path().join(utils::hintfile_name(fileid))).unwrap());
                }
            }

            // check if ids are sorted
            let fileids = utils::sorted_fileids(dir).unwrap();
            fileids.iter().enumerate().all(|(i, &v)| i as u64 == v)
        }
    }
}
