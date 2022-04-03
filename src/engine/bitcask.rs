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
    io::{self, Write},
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
use log::{LogDir, LogIndex, LogReader, LogWriter};

/// A wrapper around [`BitCask`] that implements the `KeyValueStore` trait.
#[derive(Clone, Debug)]
pub struct BitCaskKeyValueStore(pub BitCask);

impl KeyValueStore for BitCaskKeyValueStore {
    type Error = BitCaskError;

    fn set(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.0.put(key, value).map(|v| v.map(|v| v.to_vec()))
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.0.get(key).map(|v| v.map(|v| v.to_vec()))
    }

    fn del(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.0.delete(key).map(|v| v.map(|v| v.to_vec()))
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

    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, BitCaskError> {
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

    fn put(&self, key: &[u8], value: &[u8]) -> Result<Option<Bytes>, BitCaskError> {
        self.writer.lock().unwrap().put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<Option<Bytes>, BitCaskError> {
        self.writer.lock().unwrap().delete(key)
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

type KeyDir = DashMap<Bytes, KeyDirEntry>;

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
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Bytes>, BitCaskError> {
        let tstamp = utils::timestamp();
        let index = self.put_data(tstamp, key, value)?;

        let keydir_entry = KeyDirEntry {
            fileid: self.active_fileid,
            len: index.len,
            pos: index.pos,
            tstamp,
        };

        match self
            .ctx
            .keydir
            .insert(Bytes::copy_from_slice(key), keydir_entry)
        {
            Some(prev_keydir_entry) => {
                // NOTE: Unsafe usage.
                // We ensure in `BitCaskWriter` that all log entries given by KeyDir are written disk,
                // thus the readers can savely use memmap to access the data file randomly.
                let prev_datafile_entry = unsafe {
                    self.readers
                        .borrow_mut()
                        .get(&self.ctx.path, prev_keydir_entry.fileid)?
                        .at::<DataFileEntry>(prev_keydir_entry.len, prev_keydir_entry.pos)?
                };
                // NOTE: `collect_garbage` must only be called after retrieving the previous key
                // value otherwise we'll have an invalid keydir entry.
                self.collect_garbage(prev_keydir_entry.len)?;
                Ok(prev_datafile_entry.value)
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
    fn delete(&mut self, key: &[u8]) -> Result<Option<Bytes>, BitCaskError> {
        match self.ctx.keydir.remove(key) {
            Some((_, prev_keydir_entry)) => {
                // NOTE: Unsafe usage.
                // We ensure in `BitCaskWriter` that all log entries given by KeyDir are written disk,
                // thus the readers can savely use memmap to access the data file randomly.
                let prev_datafile_entry = unsafe {
                    self.readers
                        .borrow_mut()
                        .get(&self.ctx.path, prev_keydir_entry.fileid)?
                        .at::<DataFileEntry>(prev_keydir_entry.len, prev_keydir_entry.pos)?
                };
                self.put_tombstone(utils::timestamp(), key)?;
                self.collect_garbage(prev_keydir_entry.len)?;
                Ok(prev_datafile_entry.value)
            }
            None => Ok(None),
        }
    }

    /// Merge data files by copying data from previous data files to the merge data file. Old data
    /// files are deleted after the merge.
    fn merge(&mut self) -> Result<(), BitCaskError> {
        let merge_fileid = self.active_fileid + 1;
        // NOTE: We use this explicit scope to control the lifetime of `readers` which mutably
        // borrow self when `borrow_mut` is called. Both the log writers will also be dropped and
        // flushed automatically at the end of scope.
        {
            let mut merge_pos = 0;
            let mut merge_datafile_writer =
                LogWriter::create(self.ctx.path.join(utils::datafile_name(merge_fileid)))?;
            let mut merge_hintfile_writer =
                LogWriter::create(self.ctx.path.join(utils::hintfile_name(merge_fileid)))?;

            let mut readers = self.readers.borrow_mut();
            for mut keydir_entry in self.ctx.keydir.iter_mut() {
                // NOTE: Unsafe usage.
                // We ensure in `BitCaskWriter` that all log entries given by KeyDir are written disk,
                // thus the readers can savely use memmap to access the data file randomly.
                let nbytes = unsafe {
                    readers.get(&self.ctx.path, keydir_entry.fileid)?.copy_raw(
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
        }

        self.ctx
            .min_fileid
            .store(merge_fileid, atomic::Ordering::SeqCst);

        // Remove stale files from system
        let stale_fileids = utils::sorted_fileids(&self.ctx.path)?.filter(|&id| id < merge_fileid);
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

        self.new_active_datafile(self.active_fileid + 2)?;
        self.garbage = 0;
        Ok(())
    }

    /// Apppend a data entry to the active data file.
    fn put_data(
        &mut self,
        tstamp: u128,
        key: &[u8],
        value: &[u8],
    ) -> Result<LogIndex, BitCaskError> {
        let entry = DataFileEntry {
            tstamp,
            key: Bytes::copy_from_slice(key),
            value: Some(Bytes::copy_from_slice(value)),
        };

        let index = self.writer.append(&entry)?;
        self.writer.flush()?;
        self.collect_written(index.len)?;
        Ok(index)
    }

    /// Apppend a tomestone entry to the active data file.
    fn put_tombstone(&mut self, tstamp: u128, key: &[u8]) -> Result<(), BitCaskError> {
        let entry = DataFileEntry {
            tstamp,
            key: Bytes::copy_from_slice(key),
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
        self.writer =
            LogWriter::create(self.ctx.path.join(utils::datafile_name(self.active_fileid)))?;
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
    ctx: Arc<BitCaskContext>,
    readers: RefCell<LogDir>,
}

impl BitCaskReader {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, BitCaskError> {
        match self.ctx.keydir.get(key) {
            None => Ok(None),
            Some(index) => {
                let keydir_entry = index.value();
                // NOTE: We use an explicit scope to limit the lifetime of `readers` which mutably
                // borrow `self`
                let datafile_entry = {
                    // Remove stale readers
                    let mut readers = self.readers.borrow_mut();
                    readers.drop_stale(self.ctx.min_fileid.load(atomic::Ordering::SeqCst));

                    // NOTE: Unsafe usage.
                    // We ensure in `BitCaskWriter` that all log entries given by KeyDir are written disk,
                    // thus the readers can savely use memmap to access the data file randomly.
                    unsafe {
                        readers
                            .get(&self.ctx.path, keydir_entry.fileid)?
                            .at::<DataFileEntry>(keydir_entry.len, keydir_entry.pos)?
                    }
                };
                Ok(datafile_entry.value)
            }
        }
    }
}

/// Returns a list of sorted filed IDs by parsing the names of data files in the directory
/// given by `path`.
fn rebuild_keydir<P>(path: P) -> Result<(KeyDir, u64, u64), BitCaskError>
where
    P: AsRef<Path>,
{
    let keydir = KeyDir::default();
    let fileids = utils::sorted_fileids(&path)?;

    let mut active_fileid = 0;
    let mut garbage = 0;
    for fileid in fileids {
        if fileid > active_fileid {
            active_fileid = fileid;
        }

        let hintfile_path = path.as_ref().join(utils::hintfile_name(fileid));
        // use the hint file if it exists cause it has less data to be read.
        if hintfile_path.exists() {
            populate_keydir_with_hintfile(&keydir, fileid, hintfile_path)?;
        } else {
            let datafile_path = path.as_ref().join(utils::datafile_name(fileid));
            garbage += populate_keydir_with_datafile(&keydir, fileid, datafile_path)?;
        }
    }

    if active_fileid > 0 {
        active_fileid += 1;
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
