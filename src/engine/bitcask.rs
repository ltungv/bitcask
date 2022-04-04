//! An implementation of [Bitcask]. This is essentially similar to [`engine::LogStructuredHashTable`],
//! but here we closely follow the design described in the paper and try to provide a similar set of APIs.
//!
//! [Bitcask]: (https://riak.com/assets/bitcask-intro.pdf).
//! [`engine::LogStructuredHashTable`]: opal::engine::LogStructuredHashTable

mod bufio;
mod log;
mod utils;

use std::{
    cell::RefCell,
    fs,
    io::{self, BufWriter},
    path::{self, Path},
    sync::{atomic, Arc, Mutex},
};

use bytes::Bytes;
use crossbeam::{queue::ArrayQueue, utils::Backoff};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error};

use super::KeyValueStore;
use log::{LogDir, LogIndex, LogIterator, LogWriter};

/// A wrapper around [`BitCask`] that implements the `KeyValueStore` trait.
#[derive(Clone, Debug)]
pub struct BitCaskKeyValueStore(BitCask);

impl KeyValueStore for BitCaskKeyValueStore {
    type Error = BitCaskError;

    fn set(&self, key: Bytes, value: Bytes) -> Result<Option<Bytes>, Self::Error> {
        self.0.put(key, value)
    }

    fn get(&self, key: &Bytes) -> Result<Option<Bytes>, Self::Error> {
        self.0.get(key)
    }

    fn del(&self, key: &Bytes) -> Result<Option<Bytes>, Self::Error> {
        self.0.delete(key)
    }
}

impl From<BitCask> for BitCaskKeyValueStore {
    fn from(bitcask: BitCask) -> Self {
        Self(bitcask)
    }
}

/// Error returned by BitCask
#[derive(Error, Debug)]
pub enum BitCaskError {
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
        let max_datafile_size: u64 = 2 * 1024 * 1024 * 1024; // 2 GBs
        let max_garbage_size = max_datafile_size * 30 / 100; // 2 Gbs * 30%
        Self {
            max_datafile_size,
            max_garbage_size,
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
        let (index, active_fileid, garbage) = rebuild_index(&path)?;
        debug!(?active_fileid, "Got new activate file ID");

        let keydir = KeyDir {
            path: Arc::new(path.as_ref().to_path_buf()),
            min_fileid: Arc::new(atomic::AtomicU64::new(0)),
            index: Arc::new(index),
            readers: Default::default(),
        };

        let ctx = BitCaskContext {
            conf: Arc::new(conf.clone()),
            keydir,
        };

        let readers = Arc::new(ArrayQueue::new(conf.concurrency));
        for _ in 0..conf.concurrency {
            readers
                .push(BitCaskReader { ctx: ctx.clone() })
                .expect("unreachable error");
        }

        let writer = LogWriter::new(log::create(utils::datafile_name(&path, active_fileid))?)?;
        let writer = Arc::new(Mutex::new(BitCaskWriter {
            ctx,
            writer,
            active_fileid,
            written: 0,
            garbage,
        }));

        Ok(BitCask { writer, readers })
    }

    fn put(&self, key: Bytes, value: Bytes) -> Result<Option<Bytes>, BitCaskError> {
        self.writer.lock().unwrap().put(key, value)
    }

    fn delete(&self, key: &Bytes) -> Result<Option<Bytes>, BitCaskError> {
        self.writer.lock().unwrap().delete(key)
    }

    fn get(&self, key: &Bytes) -> Result<Option<Bytes>, BitCaskError> {
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

    fn merge(&self) -> Result<(), BitCaskError> {
        self.writer.lock().unwrap().merge()
    }

    fn sync_all(&self) -> Result<(), BitCaskError> {
        self.writer.lock().unwrap().sync_all()
    }
}

#[derive(Debug, Clone)]
struct KeyDirEntry {
    fileid: u64,
    len: u64,
    pos: u64,
    tstamp: u128,
}

#[derive(Serialize, Deserialize, Debug)]
struct HintFileEntry {
    tstamp: u128,
    len: u64,
    pos: u64,
    key: Bytes,
}

#[derive(Serialize, Deserialize, Debug)]
struct DataFileEntry {
    tstamp: u128,
    key: Bytes,
    value: Option<Bytes>,
}

#[derive(Debug)]
struct KeyDir {
    path: Arc<path::PathBuf>,
    min_fileid: Arc<atomic::AtomicU64>,
    index: Arc<DashMap<Bytes, KeyDirEntry>>,
    readers: RefCell<LogDir>,
}

impl KeyDir {
    fn put(
        &self,
        key: Bytes,
        fileid: u64,
        len: u64,
        pos: u64,
        tstamp: u128,
    ) -> Result<Option<(DataFileEntry, u64)>, BitCaskError> {
        let entry = KeyDirEntry {
            fileid,
            len,
            pos,
            tstamp,
        };
        self.index
            .insert(key, entry)
            .map(|keydir_entry| {
                self.read(&keydir_entry)
                    .map(|datafile_entry| (datafile_entry, keydir_entry.len))
            })
            .transpose()
    }

    fn delete(&self, key: &Bytes) -> Result<Option<(DataFileEntry, u64)>, BitCaskError> {
        self.index
            .remove(key)
            .map(|(_, keydir_entry)| {
                self.read(&keydir_entry)
                    .map(|datafile_entry| (datafile_entry, keydir_entry.len))
            })
            .transpose()
    }

    fn get(&self, key: &Bytes) -> Result<Option<DataFileEntry>, BitCaskError> {
        self.index.get(key).map(|e| self.read(&e)).transpose()
    }

    fn merge(&self, merge_fileid: u64) -> Result<(), BitCaskError> {
        let path = self.path.as_path();
        let mut merge_datafile_writer =
            BufWriter::new(log::create(utils::datafile_name(path, merge_fileid))?);
        let mut merge_hintfile_writer =
            LogWriter::new(log::create(utils::hintfile_name(path, merge_fileid))?)?;

        let mut merge_pos = 0;
        let mut readers = self.readers.borrow_mut();

        for mut keydir_entry in self.index.iter_mut() {
            // NOTE: Unsafe usage.
            // We ensure in `BitCaskWriter` that all log entries given by KeyDir are written disk,
            // thus the readers can savely use memmap to access the data file randomly.
            let nbytes = unsafe {
                readers.get(path, keydir_entry.fileid)?.copy_raw(
                    keydir_entry.len,
                    keydir_entry.pos,
                    &mut merge_datafile_writer,
                )?
            };

            // update keydir so it points to the merge data file
            keydir_entry.fileid = merge_fileid;
            keydir_entry.len = nbytes;
            keydir_entry.pos = merge_pos;

            merge_pos += nbytes;
            merge_hintfile_writer.append(&HintFileEntry {
                tstamp: keydir_entry.tstamp,
                len: keydir_entry.len,
                pos: keydir_entry.pos,
                key: keydir_entry.key().clone(),
            })?;
        }

        readers.drop_stale(merge_fileid);
        self.min_fileid
            .store(merge_fileid, atomic::Ordering::SeqCst);

        Ok(())
    }

    fn read(&self, keydir_entry: &KeyDirEntry) -> Result<DataFileEntry, BitCaskError> {
        // Remove stale readers
        let mut readers = self.readers.borrow_mut();
        readers.drop_stale(self.min_fileid.load(atomic::Ordering::SeqCst));

        // NOTE: Unsafe usage.
        // We ensure in `BitCaskWriter` that all log entries given by KeyDir are written disk,
        // thus the readers can safely use memmap to access the data file randomly.
        let prev_datafile_entry = unsafe {
            readers
                .get(self.path.as_path(), keydir_entry.fileid)?
                .at::<DataFileEntry>(keydir_entry.len, keydir_entry.pos)?
        };
        Ok(prev_datafile_entry)
    }
}

impl Clone for KeyDir {
    fn clone(&self) -> Self {
        Self {
            path: Arc::clone(&self.path),
            min_fileid: Arc::clone(&self.min_fileid),
            index: Arc::clone(&self.index),
            readers: RefCell::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct BitCaskContext {
    conf: Arc<BitCaskConfig>,
    keydir: KeyDir,
}

#[derive(Debug)]
struct BitCaskWriter {
    ctx: BitCaskContext,
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
    fn put(&mut self, key: Bytes, value: Bytes) -> Result<Option<Bytes>, BitCaskError> {
        let tstamp = utils::timestamp();
        let index = self.put_data(tstamp, &key, &value)?;
        match self
            .ctx
            .keydir
            .put(key, self.active_fileid, index.len, index.pos, tstamp)?
        {
            Some((e, garbage)) => {
                self.collect_garbage(garbage)?;
                Ok(e.value)
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
    fn delete(&mut self, key: &Bytes) -> Result<Option<Bytes>, BitCaskError> {
        match self.ctx.keydir.delete(key)? {
            Some((e, garbage)) => {
                self.put_tombstone(utils::timestamp(), key)?;
                self.collect_garbage(garbage)?;
                Ok(e.value)
            }
            None => Ok(None),
        }
    }

    /// Merge data files by copying data from previous data files to the merge data file. Old data
    /// files are deleted after the merge.
    fn merge(&mut self) -> Result<(), BitCaskError> {
        let merge_fileid = self.active_fileid + 1;
        self.ctx.keydir.merge(merge_fileid)?;

        // Remove stale files from system
        let path = self.ctx.keydir.path.as_path();
        let stale_fileids = utils::sorted_fileids(path)?.filter(|&id| id < merge_fileid);
        for id in stale_fileids {
            if let Err(e) = fs::remove_file(utils::hintfile_name(path, id)) {
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(e.into());
                }
            }
            if let Err(e) = fs::remove_file(utils::datafile_name(path, id)) {
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(e.into());
                }
            }
        }

        self.new_active_datafile(self.active_fileid + 2)?;
        self.garbage = 0;
        Ok(())
    }

    /// Apppend a data entry to the active data file.
    fn put_data(
        &mut self,
        tstamp: u128,
        key: &Bytes,
        value: &Bytes,
    ) -> Result<LogIndex, BitCaskError> {
        let entry = DataFileEntry {
            tstamp,
            key: key.clone(),
            value: Some(value.clone()),
        };

        let index = self.writer.append(&entry)?;
        self.writer.flush()?;
        self.collect_written(index.len)?;
        Ok(index)
    }

    /// Apppend a tomestone entry to the active data file.
    fn put_tombstone(&mut self, tstamp: u128, key: &Bytes) -> Result<(), BitCaskError> {
        let entry = DataFileEntry {
            tstamp,
            key: key.clone(),
            value: None,
        };

        let index = self.writer.append(&entry)?;
        self.writer.flush()?;
        self.garbage += index.len; // tombstones are wasted space
        self.collect_written(index.len)?;
        Ok(())
    }

    /// Updates the active file ID and open a new data file with the new active ID.
    fn new_active_datafile(&mut self, fileid: u64) -> Result<(), BitCaskError> {
        self.active_fileid = fileid;
        self.writer = LogWriter::new(log::create(utils::datafile_name(
            self.ctx.keydir.path.as_path(),
            self.active_fileid,
        ))?)?;
        self.written = 0;
        Ok(())
    }

    /// Add the given amount to the number of written bytes and open a new active data file when
    /// reaches the data file size threshold
    fn collect_written(&mut self, sz: u64) -> Result<(), BitCaskError> {
        self.written += sz;
        if self.written > self.ctx.conf.max_datafile_size {
            self.new_active_datafile(self.active_fileid + 1)?;
        }
        Ok(())
    }

    /// Add the given amount to the number of garbage bytes and perform merge when reaches
    /// garbage threshold.
    fn collect_garbage(&mut self, sz: u64) -> Result<(), BitCaskError> {
        self.garbage += sz;
        if self.garbage > self.ctx.conf.max_garbage_size {
            self.merge()?;
        }
        Ok(())
    }

    /// Ensure all data and metadata are synchronized with the physical disk.
    fn sync_all(&mut self) -> Result<(), BitCaskError> {
        self.writer.sync_all()?;
        Ok(())
    }
}

#[derive(Debug)]
struct BitCaskReader {
    ctx: BitCaskContext,
}

impl BitCaskReader {
    fn get(&self, key: &Bytes) -> Result<Option<Bytes>, BitCaskError> {
        match self.ctx.keydir.get(key)? {
            Some(e) => Ok(e.value),
            None => Ok(None),
        }
    }
}

/// Returns a list of sorted filed IDs by parsing the names of data files in the directory
/// given by `path`.
fn rebuild_index<P>(path: P) -> Result<(DashMap<Bytes, KeyDirEntry>, u64, u64), BitCaskError>
where
    P: AsRef<Path>,
{
    let keydir = DashMap::default();
    let fileids = utils::sorted_fileids(&path)?;

    let mut active_fileid: Option<u64> = None;
    let mut garbage = 0;
    for fileid in fileids {
        match &mut active_fileid {
            None => active_fileid = Some(fileid),
            Some(id) => {
                if fileid > *id {
                    *id = fileid;
                }
            }
        }
        match log::open(utils::hintfile_name(&path, fileid)) {
            Ok(f) => populate_keydir_with_hintfile(&keydir, f, fileid)?,
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => {
                    garbage += populate_keydir_with_datafile(
                        &keydir,
                        log::open(utils::datafile_name(&path, fileid))?,
                        fileid,
                    )?;
                }
                _ => return Err(e.into()),
            },
        }
    }

    let active_fileid = active_fileid.map(|id| id + 1).unwrap_or_default();
    Ok((keydir, active_fileid, garbage))
}

fn populate_keydir_with_hintfile(
    keydir: &DashMap<Bytes, KeyDirEntry>,
    file: fs::File,
    fileid: u64,
) -> Result<(), BitCaskError> {
    let mut hintfile_iter = LogIterator::new(file)?;
    while let Some((_, entry)) = hintfile_iter.next::<HintFileEntry>()? {
        // Indices given by the hint file do not point to tombstones.
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

fn populate_keydir_with_datafile(
    keydir: &DashMap<Bytes, KeyDirEntry>,
    file: fs::File,
    fileid: u64,
) -> Result<u64, BitCaskError> {
    let mut garbage = 0;
    let mut datafile_iter = LogIterator::new(file)?;
    while let Some((datafile_index, datafile_entry)) = datafile_iter.next::<DataFileEntry>()? {
        match datafile_entry.value {
            None => garbage += datafile_index.len,
            Some(_) => {
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
