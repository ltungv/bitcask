//! An implementation of [Bitcask]. This is essentially similar to [`engine::LogStructuredHashTable`],
//! but here we closely follow the design described in the paper and try to provide a similar set of APIs.
//!
//! [Bitcask]: (https://riak.com/assets/bitcask-intro.pdf).
//! [`engine::LogStructuredHashTable`]: opal::engine::LogStructuredHashTable

pub(crate) mod bufio;
pub(crate) mod datafile;
pub(crate) mod hintfile;

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fs, io,
    path::{self, Path},
    sync::{atomic, Arc, Mutex},
    time,
};

use crossbeam::{queue::ArrayQueue, utils::Backoff};
use dashmap::DashMap;
use thiserror::Error;

use super::KeyValueStore;
use datafile::{
    DataFileEntryValue, DataFileError, DataFileIterator, DataFileReader, DataFileWriter,
};
use hintfile::{HintFileError, HintFileIterator, HintFileWriter};

const DATAFILE_EXT: &str = "bitcask.data";

const HINTFILE_EXT: &str = "bitcask.hint";

/// Merge log files when then number of unused bytes across all files exceeds this limit.
const GARBAGE_THRESHOLD: u64 = 4 * 1024 * 1024; // 4MB

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
    #[error("entry `{0:?}` is invalid")]
    BadKeyDirEntry(KeyDirEntry),

    #[error(transparent)]
    DataFile(#[from] DataFileError),

    #[error(transparent)]
    HintFile(#[from] HintFileError),

    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Configuration for a `BitCask` instance.
#[derive(Debug)]
pub struct BitCaskConfig {
    concurrency: usize,
}

impl BitCaskConfig {
    pub fn open<P>(&self, path: P) -> Result<BitCask, BitCaskError>
    where
        P: AsRef<Path>,
    {
        BitCask::open(path, self)
    }

    pub fn concurrency(&mut self, concurrency: usize) -> &mut Self {
        self.concurrency = concurrency;
        self
    }
}

impl Default for BitCaskConfig {
    fn default() -> Self {
        Self {
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
        let ctx = Arc::new(BitCaskContext {
            path: path.as_ref().to_path_buf(),
            min_fileid: atomic::AtomicU64::new(0),
            keydir,
        });

        let readers = Arc::new(ArrayQueue::new(conf.concurrency));
        for _ in 0..conf.concurrency {
            readers
                .push(BitCaskReader {
                    ctx: Arc::clone(&ctx),
                    readers: RefCell::new(ReaderMap::new()),
                })
                .expect("unreachable error");
        }

        let writer = DataFileWriter::create(path.as_ref().join(datafile_name(active_fileid)))?;
        let writer = Arc::new(Mutex::new(BitCaskWriter {
            ctx,
            readers: RefCell::new(ReaderMap::new()),
            writer,
            active_fileid,
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

    fn sync(&self) -> Result<(), BitCaskError> {
        self.writer.lock().unwrap().sync()
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

#[derive(Debug)]
struct ReaderMap(BTreeMap<u64, DataFileReader>);

impl ReaderMap {
    fn new() -> Self {
        Self(BTreeMap::new())
    }

    fn get<P>(&mut self, path: P, fileid: u64) -> Result<&DataFileReader, BitCaskError>
    where
        P: AsRef<Path>,
    {
        let reader = self.0.entry(fileid).or_insert(DataFileReader::open(
            path.as_ref().join(datafile_name(fileid)),
        )?);
        Ok(reader)
    }

    fn stale_fileids(&self, min_fileid: u64) -> Vec<u64> {
        self.0
            .keys()
            .filter(|&&id| id < min_fileid)
            .cloned()
            .collect()
    }

    fn drop_stale(&mut self, min_fileid: u64) {
        self.stale_fileids(min_fileid).iter().for_each(|id| {
            self.0.remove(id);
        });
    }
}

#[derive(Debug)]
struct BitCaskContext {
    path: path::PathBuf,
    min_fileid: atomic::AtomicU64,
    keydir: KeyDir,
}

#[derive(Debug)]
struct BitCaskWriter {
    ctx: Arc<BitCaskContext>,
    readers: RefCell<ReaderMap>,
    writer: DataFileWriter,
    active_fileid: u64,
    garbage: u64,
}

impl BitCaskWriter {
    /// Set the value of a key, overwriting any existing value at that key.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, BitCaskError> {
        let tstamp = timestamp();
        let index = self.writer.data(tstamp, key, value)?;

        let keydir_entry = KeyDirEntry {
            fileid: self.active_fileid,
            len: index.len,
            pos: index.pos,
            tstamp,
        };

        match self.ctx.keydir.insert(key.to_vec(), keydir_entry) {
            Some(prev_index) => {
                let prev_datafile_entry = {
                    let readers = self.readers.get_mut();
                    let reader = readers.get(&self.ctx.path, prev_index.fileid)?;
                    unsafe { reader.entry(prev_index.len, prev_index.pos)? }
                };
                self.gc(prev_index.len)?;
                match prev_datafile_entry.value {
                    DataFileEntryValue::Data(v) => Ok(Some(v)),
                    DataFileEntryValue::Tombstone => Err(BitCaskError::BadKeyDirEntry(prev_index)),
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
            Some((_, prev_index)) => {
                let prev_datafile_entry = {
                    let readers = self.readers.get_mut();
                    let reader = readers.get(&self.ctx.path, prev_index.fileid)?;
                    unsafe { reader.entry(prev_index.len, prev_index.pos)? }
                };
                // Write a tombstone value
                self.writer.tombstone(timestamp(), key)?;
                // Accumulated unused bytes count when we delete the given key
                self.gc(prev_index.len)?;
                match prev_datafile_entry.value {
                    DataFileEntryValue::Data(v) => Ok(Some(v)),
                    DataFileEntryValue::Tombstone => Err(BitCaskError::BadKeyDirEntry(prev_index)),
                }
            }
            None => Ok(None),
        }
    }

    fn sync(&mut self) -> Result<(), BitCaskError> {
        self.writer.sync()?;
        Ok(())
    }

    fn gc(&mut self, sz: u64) -> Result<(), BitCaskError> {
        self.garbage += sz;
        if self.garbage > GARBAGE_THRESHOLD {
            self.merge()?;
        }
        Ok(())
    }

    fn merge(&mut self) -> Result<(), BitCaskError> {
        let merge_fileid = self.active_fileid + 1;
        let mut merge_datafile_writer =
            DataFileWriter::create(self.ctx.path.join(datafile_name(merge_fileid)))?;
        let mut merge_hintfile_writer =
            HintFileWriter::create(self.ctx.path.join(hintfile_name(merge_fileid)))?;

        let mut readers = self.readers.borrow_mut();
        for mut keydir_entry in self.ctx.keydir.iter_mut() {
            let merge_pos = merge_datafile_writer.pos();
            let reader = readers.get(&self.ctx.path, keydir_entry.fileid)?;
            unsafe {
                reader.copy(
                    keydir_entry.len,
                    keydir_entry.pos,
                    merge_datafile_writer.writer(),
                )?
            };

            keydir_entry.fileid = merge_fileid;
            keydir_entry.pos = merge_pos;

            merge_hintfile_writer.append(
                keydir_entry.tstamp,
                keydir_entry.len,
                keydir_entry.pos,
                keydir_entry.key(),
            )?;
        }

        let stale_fileids = readers.stale_fileids(merge_fileid);
        readers.drop_stale(merge_fileid);

        for id in stale_fileids {
            let hintfile_path = self.ctx.path.join(hintfile_name(id));
            if hintfile_path.exists() {
                fs::remove_file(hintfile_path)?;
            }

            let datafile_path = self.ctx.path.join(hintfile_name(id));
            fs::remove_file(datafile_path)?;
        }

        self.garbage = 0;
        self.active_fileid += 2;
        self.writer =
            DataFileWriter::create(self.ctx.path.join(datafile_name(self.active_fileid)))?;
        Ok(())
    }
}

#[derive(Debug)]
struct BitCaskReader {
    ctx: Arc<BitCaskContext>,
    readers: RefCell<ReaderMap>,
}

impl BitCaskReader {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BitCaskError> {
        match self.ctx.keydir.get(key) {
            None => Ok(None),
            Some(index) => {
                self.drop_stale_readers();
                Ok(Some(self.get_by_index(index.value())?))
            }
        }
    }

    fn get_by_index(&self, keydir_entry: &KeyDirEntry) -> Result<Vec<u8>, BitCaskError> {
        let datafile_entry = {
            let mut readers = self.readers.borrow_mut();
            let reader = readers.get(&self.ctx.path, keydir_entry.fileid)?;
            unsafe { reader.entry(keydir_entry.len, keydir_entry.pos)? }
        };

        match datafile_entry.value {
            DataFileEntryValue::Data(v) => Ok(v),
            DataFileEntryValue::Tombstone => {
                Err(BitCaskError::BadKeyDirEntry(keydir_entry.clone()))
            }
        }
    }

    fn drop_stale_readers(&self) {
        let min_fileid = self.ctx.min_fileid.load(atomic::Ordering::SeqCst);
        let mut readers = self.readers.borrow_mut();
        readers.drop_stale(min_fileid);
    }
}

fn rebuild_keydir<P>(path: P) -> Result<(KeyDir, u64, u64), BitCaskError>
where
    P: AsRef<Path>,
{
    let keydir = KeyDir::default();
    let fileids = sorted_fileids(&path)?;
    let active_fileid = fileids.last().map(|id| id + 1).unwrap_or_default();

    let mut garbage = 0;
    for fileid in fileids {
        let hintfile_path = path.as_ref().join(hintfile_name(fileid));
        if hintfile_path.exists() {
            populate_keydir_with_hintfile(&keydir, fileid, hintfile_path)?;
        } else {
            let datafile_path = path.as_ref().join(datafile_name(fileid));
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
    let hintfile_iter = HintFileIterator::open(path)?;
    for parse_result in hintfile_iter {
        let entry = parse_result?;
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
    let mut datafile_iter = DataFileIterator::open(path)?;
    while let Some((datafile_index, datafile_entry)) = datafile_iter.entry()? {
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

fn datafile_name(fileid: u64) -> String {
    format!("{}.{}", fileid, DATAFILE_EXT)
}

fn hintfile_name(fileid: u64) -> String {
    format!("{}.{}", fileid, HINTFILE_EXT)
}

fn sorted_fileids<P>(path: P) -> Result<Vec<u64>, BitCaskError>
where
    P: AsRef<Path>,
{
    // read directory
    let fileids: BTreeSet<u64> = fs::read_dir(&path)?
        // ignore errors
        .filter_map(std::result::Result::ok)
        // extract paths
        .map(|e| e.path())
        // get files with data file extentions
        .filter(|p| p.is_file() && p.extension() == Some(OsStr::new(DATAFILE_EXT)))
        // parse the file id as u64
        .filter_map(|p| p.file_stem().and_then(OsStr::to_str).map(str::parse::<u64>))
        .filter_map(std::result::Result::ok)
        .collect();

    let fileids: Vec<u64> = fileids.into_iter().collect();
    Ok(fileids)
}

fn timestamp() -> u128 {
    time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .expect("invalid system time")
        .as_nanos()
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
                tmps.push(fs::File::create(dir.path().join(datafile_name(fileid))).unwrap());
                if rand::random() {
                    tmps.push(fs::File::create(dir.path().join(hintfile_name(fileid))).unwrap());
                }
            }

            // check if ids are sorted
            let fileids = sorted_fileids(dir).unwrap();
            fileids.iter().enumerate().all(|(i, &v)| i as u64 == v)
        }
    }
}
