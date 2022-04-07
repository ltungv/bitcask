//! An implementation of [Bitcask](https://riak.com/assets/bitcask-intro.pdf).

mod bufio;
mod config;
mod log;
mod utils;

pub use config::Config;

use std::{
    cell::RefCell,
    fs,
    io::{self, BufWriter},
    path::{self, Path},
    sync::Arc,
};

use bytes::Bytes;
use crossbeam::{atomic::AtomicCell, queue::ArrayQueue, utils::Backoff};
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error};

use super::KeyValueStorage;
use log::{LogDir, LogIterator, LogWriter};

/// Error returned by Bitcask
#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error - {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error - {0}")]
    Serialization(#[from] bincode::Error),
}

/// A wrapper around [`Bitcask`] that implements the `KeyValueStore` trait.
#[derive(Clone, Debug)]
pub struct BitcaskKeyValueStorage(Bitcask);

impl KeyValueStorage for BitcaskKeyValueStorage {
    type Error = Error;

    fn del(&self, key: Bytes) -> Result<bool, Self::Error> {
        self.0.delete(key)
    }

    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Error> {
        self.0.get(key)
    }

    fn set(&self, key: Bytes, value: Bytes) -> Result<(), Self::Error> {
        self.0.put(key, value)
    }
}

impl From<Bitcask> for BitcaskKeyValueStorage {
    fn from(bitcask: Bitcask) -> Self {
        Self(bitcask)
    }
}

/// Implementation of a Bitcask instance.
///
/// The APIs resembles the one given in [bitcask-intro.pdf](https://riak.com/assets/bitcask-intro.pdf)
/// with a few functions omitted.
///
/// Each Bitcask instance is a directory containing data files. At any moment, one file is "active"
/// for writing, and Bitcask sequentially appends data to the active data file. Bitcask keeps a
/// "keydir" that maps a key to the position of its value in the data files and uses the keydir to
/// access the data file entries directly without having to scan all data files.
#[derive(Clone, Debug)]
pub struct Bitcask {
    /// A mutex-protected writer used for appending data entry to the active data files. All
    /// operations that make changes to the active data file are delegated to this object.
    writer: Arc<Mutex<Writer>>,

    /// A readers queue for parallelizing read-access to the key-value store. Upon a read-access,
    /// a reader is taken from the queue and used for reading the data files. Once we finish
    /// reading, the read is returned back to the queue.
    readers: Arc<ArrayQueue<Reader>>,
}

#[derive(Debug, Clone)]
struct Context {
    conf: Arc<Config>,
    path: Arc<path::PathBuf>,
    min_fileid: Arc<AtomicCell<u64>>,
    keydir: Arc<DashMap<Bytes, KeyDirEntry>>,
}

#[derive(Debug)]
struct Writer {
    ctx: Context,
    readers: RefCell<LogDir>,
    writer: LogWriter,
    active_fileid: u64,
    written_bytes: u64,
    dead_bytes: u64,
}

#[derive(Debug)]
struct Reader {
    ctx: Context,
    readers: RefCell<LogDir>,
}

impl Bitcask {
    fn open<P>(path: P, conf: Config) -> Result<Bitcask, Error>
    where
        P: AsRef<Path>,
    {
        let (keydir, active_fileid, garbage) = rebuild_index(&path)?;
        debug!(?active_fileid, "Got new activate file ID");

        let readers = if conf.concurrency > 0 {
            Arc::new(ArrayQueue::new(conf.concurrency))
        } else {
            Arc::new(ArrayQueue::new(conf.concurrency + 1))
        };

        let ctx = Context {
            conf: Arc::new(conf),
            path: Arc::new(path.as_ref().to_path_buf()),
            min_fileid: Arc::new(AtomicCell::new(0)),
            keydir: Arc::new(keydir),
        };

        for _ in 0..readers.capacity() {
            readers
                .push(Reader {
                    ctx: ctx.clone(),
                    readers: RefCell::default(),
                })
                .expect("unreachable error");
        }

        let writer = Arc::new(Mutex::new(Writer {
            ctx,
            readers: RefCell::default(),
            writer: LogWriter::new(log::create(utils::datafile_name(&path, active_fileid))?)?,
            active_fileid,
            written_bytes: 0,
            dead_bytes: garbage,
        }));

        Ok(Bitcask { writer, readers })
    }

    fn put(&self, key: Bytes, value: Bytes) -> Result<(), Error> {
        self.writer.lock().put(key, value)
    }

    fn delete(&self, key: Bytes) -> Result<bool, Error> {
        self.writer.lock().delete(key)
    }

    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Error> {
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
}

impl Writer {
    /// Set the value of a key and overwrite any existing value. If a value is overwritten
    /// return it, otherwise return None.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn put(&mut self, key: Bytes, value: Bytes) -> Result<(), Error> {
        let tstamp = utils::timestamp();
        let keydir_entry = self.write(tstamp, key.clone(), Some(value))?;
        if let Some(prev_keydir_entry) = self.ctx.keydir.insert(key, keydir_entry) {
            self.collect_dead_bytes(prev_keydir_entry.len)?;
        }
        Ok(())
    }

    /// Delete a key and returning its value, if it exists, otherwise return `None`.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn delete(&mut self, key: Bytes) -> Result<bool, Error> {
        self.write(utils::timestamp(), key.clone(), None)?;
        match self.ctx.keydir.remove(&key) {
            Some((_, prev_keydir_entry)) => {
                self.collect_dead_bytes(prev_keydir_entry.len)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Merge data files by copying data from previous data files to the merge data file. Old data
    /// files are deleted after the merge.
    #[tracing::instrument(skip(self))]
    fn merge(&mut self) -> Result<(), Error> {
        let path = self.ctx.path.as_path();
        let min_merge_fileid = self.active_fileid + 1;
        let mut merge_fileid = min_merge_fileid;
        debug!(merge_fileid, "new merge file");

        // NOTE: we use an explicit scope here to control the lifetimes of `readers`,
        // `merge_datafile_writer` and `merge_hintfile_writer`. We drop the readers
        // early so we can later mutably borrow `self` and drop the writers early so
        // they are flushed.
        {
            let mut merge_pos = 0;
            let mut merge_datafile_writer =
                BufWriter::new(log::create(utils::datafile_name(path, merge_fileid))?);
            let mut merge_hintfile_writer =
                LogWriter::new(log::create(utils::hintfile_name(path, merge_fileid))?)?;

            let mut readers = self.readers.borrow_mut();
            for mut keydir_entry in self.ctx.keydir.iter_mut() {
                // SAFETY: We ensure in `BitcaskWriter` that all log entries given by
                // KeyDir are written disk, thus the readers can savely use memmap to
                // access the data file randomly.
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

                if merge_pos > self.ctx.conf.max_file_size {
                    merge_pos = 0;
                    merge_fileid += 1;
                    merge_datafile_writer =
                        BufWriter::new(log::create(utils::datafile_name(path, merge_fileid))?);
                    merge_hintfile_writer =
                        LogWriter::new(log::create(utils::hintfile_name(path, merge_fileid))?)?;
                    debug!(merge_fileid, "new merge file");
                }
            }
            readers.drop_stale(merge_fileid);
        }

        self.ctx.min_fileid.store(min_merge_fileid);

        // Remove stale files from system
        let stale_fileids = utils::sorted_fileids(path)?.filter(|&id| id < min_merge_fileid);
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

        self.new_active_datafile(merge_fileid + 1)?;
        self.dead_bytes = 0;
        Ok(())
    }

    #[tracing::instrument(skip(self, key, value))]
    fn write(
        &mut self,
        tstamp: u128,
        key: Bytes,
        value: Option<Bytes>,
    ) -> Result<KeyDirEntry, Error> {
        let datafile_entry = DataFileEntry { tstamp, key, value };
        let index = self.writer.append(&datafile_entry)?;
        let keydir_entry = KeyDirEntry {
            fileid: self.active_fileid,
            len: index.len,
            pos: index.pos,
            tstamp,
        };
        if datafile_entry.value.is_none() {
            self.dead_bytes += keydir_entry.len; // tombstones are wasted space
        }
        self.collect_written(keydir_entry.len)?;
        Ok(keydir_entry)
    }

    /// Updates the active file ID and open a new data file with the new active ID.
    #[tracing::instrument(skip(self))]
    fn new_active_datafile(&mut self, fileid: u64) -> Result<(), Error> {
        self.active_fileid = fileid;
        self.writer = LogWriter::new(log::create(utils::datafile_name(
            self.ctx.path.as_path(),
            self.active_fileid,
        ))?)?;
        self.written_bytes = 0;
        Ok(())
    }

    /// Add the given amount to the number of written bytes and open a new active data file when
    /// reaches the data file size threshold
    fn collect_written(&mut self, sz: u64) -> Result<(), Error> {
        self.written_bytes += sz;
        if self.written_bytes > self.ctx.conf.max_file_size {
            self.new_active_datafile(self.active_fileid + 1)?;
        }
        Ok(())
    }

    /// Add the given amount to the number of garbage bytes and perform merge when reaches
    /// garbage threshold.
    fn collect_dead_bytes(&mut self, sz: u64) -> Result<(), Error> {
        self.dead_bytes += sz;
        let MergeStrategy::DeadBytes(n) = self.ctx.conf.merge_strategy;
        if self.dead_bytes > n {
            self.merge()?;
        }
        Ok(())
    }
}

impl Reader {
    /// Get the value of a key and return it, if it exists, otherwise return return `None`.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    #[tracing::instrument(skip(self, key))]
    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Error> {
        match self.ctx.keydir.get(&key) {
            Some(keydir_entry) => {
                let mut readers = self.readers.borrow_mut();
                readers.drop_stale(self.ctx.min_fileid.load());

                // SAFETY: We have taken `keydir_entry` from KeyDir which is ensured to point to
                // valid data file positions. Thus we can be confident that the Mmap won't be
                // mapped to an invalid segment.
                let datafile_entry = unsafe {
                    readers
                        .get(self.ctx.path.as_path(), keydir_entry.fileid)?
                        .at::<DataFileEntry>(keydir_entry.len, keydir_entry.pos)?
                };

                Ok(datafile_entry.value)
            }
            None => Ok(None),
        }
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

/// Returns a list of sorted filed IDs by parsing the names of data files in the directory
/// given by `path`.
fn rebuild_index<P>(path: P) -> Result<(DashMap<Bytes, KeyDirEntry>, u64, u64), Error>
where
    P: AsRef<Path>,
{
    let keydir = DashMap::default();
    let fileids = utils::sorted_fileids(&path)?;

    let mut active_fileid = None;
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
) -> Result<(), Error> {
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
) -> Result<u64, Error> {
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

#[cfg(test)]
mod tests {
    use proptest::{collection, prelude::*};

    use super::*;

    #[test]
    fn bitcask_seq_read_after_write_should_return_the_written_data() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Config::default().concurrency(1).open(dir.path()).unwrap();

        proptest!(|(key in collection::vec(any::<u8>(), 0..64),
                    value in collection::vec(any::<u8>(), 0..256))| {
            kv.put(Bytes::from(key.clone()), Bytes::from(value.clone())).unwrap();
            let value_from_kv = kv.get(Bytes::from(key)).unwrap();
            prop_assert_eq!(Some(Bytes::from(value)), value_from_kv);
        });
    }
}
