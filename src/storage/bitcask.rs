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

/// An implementation of a Bitcask instance whose APIs resemble the one given in
/// [bitcask-intro.pdf] but with a few methods omitted.
///
/// Each Bitcask instance is a directory containing data files. At any moment, one file is "active"
/// for writing, and Bitcask sequentially appends data to the active data file. Bitcask keeps a
/// "keydir" that maps a key to the position of its value in the data files and uses the keydir to
/// access the data file entries directly without having to scan all data files.
///
/// Operations on the Bitcask instance are not directly handled by this struct. Instead, it gives
/// out handles to the Bitcask instance to the threads that need it, and operations on the instance
/// are concurrently executed through these handles.
///
/// [bitcask-intro.pdf]: https://riak.com/assets/bitcask-intro.pdf
pub struct Bitcask {
    handle: Handle,
}

/// A handle to the Bitcask instance that allows multiple different threads to safely access it.
#[derive(Clone, Debug)]
pub struct Handle {
    /// A mutex-protected writer used for appending data entry to the active data files. All
    /// operations that make changes to the active data file are delegated to this object.
    writer: Arc<Mutex<Writer>>,

    /// A readers queue for parallelizing read-access to the key-value store. Upon a read-access,
    /// a reader is taken from the queue and used for reading the data files. Once we finish
    /// reading, the read is returned back to the queue.
    readers: Arc<ArrayQueue<Reader>>,
}

/// The context holds states that are shared across both reads and writes operations.
#[derive(Debug, Clone)]
struct Context {
    conf: Arc<Config>,
    path: Arc<path::PathBuf>,
    min_fileid: Arc<AtomicCell<u64>>,
    keydir: Arc<DashMap<Bytes, KeyDirEntry>>,
    stats: Arc<DashMap<u64, LogStatistics>>,
}

/// The writer appends log entries to data files and ensures that indices in KeyDir point to a valid
/// file locations.
#[derive(Debug)]
struct Writer {
    ctx: Context,
    readers: RefCell<LogDir>,
    writer: LogWriter,
    active_fileid: u64,
    written_bytes: u64,
}

/// The reader reads log entries from data files given the locations found in KeyDir. Since data files
/// are immutable (except for the active one), we can safely read them concurrently without extra
/// synchronizations between threads.
#[derive(Debug)]
struct Reader {
    ctx: Context,
    readers: RefCell<LogDir>,
}

impl Bitcask {
    pub fn get_handle(&self) -> Handle {
        self.handle.clone()
    }
}

impl KeyValueStorage for Handle {
    type Error = Error;

    fn del(&self, key: Bytes) -> Result<bool, Self::Error> {
        self.delete(key)
    }

    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Error> {
        self.get(key)
    }

    fn set(&self, key: Bytes, value: Bytes) -> Result<(), Self::Error> {
        self.put(key, value)
    }
}

impl Handle {
    fn open<P>(path: P, conf: Config) -> Result<Handle, Error>
    where
        P: AsRef<Path>,
    {
        let (keydir, stats, active_fileid) = rebuild_index(&path)?;
        debug!(?active_fileid, "got new active file ID");

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
            stats: Arc::new(stats),
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
        }));

        Ok(Handle { writer, readers })
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
        let keydir_entry = self.write(utils::timestamp(), key.clone(), Some(value))?;
        if let Some(prev_keydir_entry) = self.ctx.keydir.insert(key, keydir_entry) {
            self.ctx
                .stats
                .entry(prev_keydir_entry.fileid)
                .or_default()
                .overwrite(prev_keydir_entry.len);
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
                self.ctx
                    .stats
                    .entry(prev_keydir_entry.fileid)
                    .or_default()
                    .overwrite(prev_keydir_entry.len);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn write(
        &mut self,
        tstamp: u128,
        key: Bytes,
        value: Option<Bytes>,
    ) -> Result<KeyDirEntry, Error> {
        // Append log entry a create a KeyDir entry for it
        let datafile_entry = DataFileEntry { tstamp, key, value };
        let index = self.writer.append(&datafile_entry)?;
        let keydir_entry = KeyDirEntry {
            fileid: self.active_fileid,
            len: index.len,
            pos: index.pos,
            tstamp,
        };
        self.written_bytes += index.len;

        // Collect statistics of the active data file for the merging process. If we add
        // a value to a key, we increase the number of live keys. If we add a tombstone,
        // we increase the number of dead keys.
        {
            let mut stats = self.ctx.stats.entry(self.active_fileid).or_default();
            if datafile_entry.value.is_some() {
                stats.add_live();
            } else {
                stats.add_dead(index.len);
            }
            debug!(
                entry_len = %keydir_entry.len,
                entry_pos = %keydir_entry.pos,
                active_fileid = %keydir_entry.fileid,
                active_file_size = %self.written_bytes,
                active_live_keys = %stats.live_keys,
                active_dead_keys = %stats.dead_keys,
                active_dead_bytes = %stats.dead_bytes,
                "appended new log entry"
            );
        }

        // Check if it exceeds the max limit.
        if self.written_bytes > self.ctx.conf.max_file_size.as_u64() {
            self.new_active_datafile(self.active_fileid + 1)?;
        }
        Ok(keydir_entry)
    }

    /// Merge data files by copying data from previous data files to the merge data file. Old data
    /// files are deleted after the merge.
    #[tracing::instrument(level = "debug", skip(self))]
    fn merge(&mut self) -> Result<(), Error> {
        let path = self.ctx.path.as_path();
        let min_merge_fileid = self.active_fileid + 1;
        let mut merge_fileid = min_merge_fileid;
        debug!(merge_fileid, "new merge file");

        // TODO: periodically check whether we need to merge given the triggers. Once the conditions
        // given the the triggers are met, we should only merge files that met the conditions given
        // by the thresholds.

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

                if merge_pos > self.ctx.conf.max_file_size.as_u64() {
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
        // TODO: remove the statistics of files that have been merged
        // self.dead_bytes = 0;
        Ok(())
    }

    /// Updates the active file ID and open a new data file with the new active ID.
    #[tracing::instrument(level = "debug", skip(self))]
    fn new_active_datafile(&mut self, fileid: u64) -> Result<(), Error> {
        self.active_fileid = fileid;
        self.writer = LogWriter::new(log::create(utils::datafile_name(
            self.ctx.path.as_path(),
            self.active_fileid,
        ))?)?;
        self.written_bytes = 0;
        Ok(())
    }
}

impl Reader {
    /// Get the value of a key and return it, if it exists, otherwise return return `None`.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    #[tracing::instrument(level = "debug", skip(self))]
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

/// Keeping track of the number of live/dead keys and how much space do the dead keys occupy.
#[derive(Debug, Default)]
struct LogStatistics {
    live_keys: u64,
    dead_keys: u64,
    dead_bytes: u64,
}

impl LogStatistics {
    fn add_live(&mut self) {
        self.live_keys += 1;
    }

    fn add_dead(&mut self, nbytes: u64) {
        self.dead_keys += 1;
        self.dead_bytes += nbytes;
    }

    fn overwrite(&mut self, nbytes: u64) {
        self.live_keys -= 1;
        self.dead_keys += 1;
        self.dead_bytes += nbytes;
    }
}

/// Read the given directory, rebuild the KeyDir, and gather statistics about the Bitcask instance
/// at that directory.
#[allow(clippy::type_complexity)]
fn rebuild_index<P>(
    path: P,
) -> Result<
    (
        DashMap<Bytes, KeyDirEntry>,
        DashMap<u64, LogStatistics>,
        u64,
    ),
    Error,
>
where
    P: AsRef<Path>,
{
    let keydir = DashMap::default();
    let stats = DashMap::default();
    let fileids = utils::sorted_fileids(&path)?;

    let mut active_fileid = None;
    for fileid in fileids {
        // Collect the most recently created file id.
        match &mut active_fileid {
            None => active_fileid = Some(fileid),
            Some(id) => {
                if fileid > *id {
                    *id = fileid;
                }
            }
        }
        if let Err(e) = populate_keydir_with_hintfile(&path, fileid, &keydir, &stats) {
            match e {
                Error::Io(ref ioe) => match ioe.kind() {
                    io::ErrorKind::NotFound => {
                        // Read the data file if the hint file does not exist.
                        populate_keydir_with_datafile(&path, fileid, &keydir, &stats)?;
                    }
                    _ => return Err(e),
                },
                _ => return Err(e),
            }
        }
    }

    let active_fileid = active_fileid.map(|id| id + 1).unwrap_or_default();
    Ok((keydir, stats, active_fileid))
}

fn populate_keydir_with_hintfile<P>(
    path: P,
    fileid: u64,
    keydir: &DashMap<Bytes, KeyDirEntry>,
    stats: &DashMap<u64, LogStatistics>,
) -> Result<(), Error>
where
    P: AsRef<Path>,
{
    let file = log::open(utils::hintfile_name(&path, fileid))?;
    let mut hintfile_iter = LogIterator::new(file)?;
    while let Some((_, entry)) = hintfile_iter.next::<HintFileEntry>()? {
        let keydir_entry = KeyDirEntry {
            fileid,
            len: entry.len,
            pos: entry.pos,
            tstamp: entry.tstamp,
        };
        stats.entry(fileid).or_default().add_live();
        if let Some(prev_keydir_entry) = keydir.insert(entry.key, keydir_entry) {
            stats
                .entry(prev_keydir_entry.fileid)
                .or_default()
                .overwrite(prev_keydir_entry.len);
        }
    }
    Ok(())
}

fn populate_keydir_with_datafile<P>(
    path: P,
    fileid: u64,
    keydir: &DashMap<Bytes, KeyDirEntry>,
    stats: &DashMap<u64, LogStatistics>,
) -> Result<(), Error>
where
    P: AsRef<Path>,
{
    let file = log::open(utils::datafile_name(&path, fileid))?;
    let mut datafile_iter = LogIterator::new(file)?;
    while let Some((datafile_index, datafile_entry)) = datafile_iter.next::<DataFileEntry>()? {
        match datafile_entry.value {
            None => stats
                .entry(fileid)
                .or_default()
                .add_dead(datafile_index.len),
            Some(_) => {
                let keydir_entry = KeyDirEntry {
                    fileid,
                    len: datafile_index.len,
                    pos: datafile_index.pos,
                    tstamp: datafile_entry.tstamp,
                };
                stats.entry(fileid).or_default().add_live();
                if let Some(prev_keydir_entry) = keydir.insert(datafile_entry.key, keydir_entry) {
                    stats
                        .entry(prev_keydir_entry.fileid)
                        .or_default()
                        .overwrite(prev_keydir_entry.len);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use proptest::{collection, prelude::*};

    use super::*;

    #[test]
    fn bitcask_sequential_read_after_write_should_return_the_written_data() {
        let dir = tempfile::tempdir().unwrap();
        let conf = Config::default().concurrency(1).to_owned();
        let kv = conf.open(dir.path()).unwrap();
        let handle = kv.get_handle();

        proptest!(|(key in collection::vec(any::<u8>(), 0..64),
                    value in collection::vec(any::<u8>(), 0..256))| {
            handle.put(Bytes::from(key.clone()), Bytes::from(value.clone())).unwrap();
            let value_from_kv = handle.get(Bytes::from(key)).unwrap();
            prop_assert_eq!(Some(Bytes::from(value)), value_from_kv);
        });
    }

    #[test]
    fn bitcask_rebuilt_keydir_correctly() {
        let dir = tempfile::tempdir().unwrap();
        // create lots of small files to test reading across different files
        let conf = Config::default()
            .concurrency(1)
            .max_file_size(ByteSize::kib(64))
            .to_owned();
        {
            let kv = conf.clone().open(dir.path()).unwrap();
            let handle = kv.get_handle();
            // put 10000 different keys
            for i in 0..10000 {
                handle
                    .put(
                        Bytes::from(format!("key{}", i)),
                        Bytes::from(format!("value{}", i)),
                    )
                    .unwrap();
            }
        }

        // rebuild bitcask
        let kv = conf.open(dir.path()).unwrap();
        let handle = kv.get_handle();
        // get 10000 different keys
        for i in 0..10000 {
            let value = handle
                .get(Bytes::from(format!("key{}", i)))
                .unwrap()
                .unwrap();
            assert_eq!(Bytes::from(format!("value{}", i)), value);
        }
    }

    #[test]
    fn bitcask_rebuilt_stats_correctly() {
        let dir = tempfile::tempdir().unwrap();
        // create lots of small files to test reading across different files
        let conf = Config::default()
            .concurrency(1)
            .max_file_size(ByteSize::kib(64))
            .to_owned();

        {
            let kv = conf.clone().open(dir.path()).unwrap();
            let handle = kv.get_handle();
            // put 10000 different keys
            for i in 0..10000 {
                handle
                    .put(
                        Bytes::from(format!("key{}", i)),
                        Bytes::from(format!("value{}", i)),
                    )
                    .unwrap();
            }
            // overwrite 5000 keys
            for i in 0..5000 {
                handle
                    .put(
                        Bytes::from(format!("key{}", i)),
                        Bytes::from(format!("value{}", i)),
                    )
                    .unwrap();
            }
        }

        // rebuild bitcask
        let kv = conf.open(dir.path()).unwrap();
        let handle = kv.get_handle();
        // should get 10000 live keys and 5000 dead keys.
        let mut lives = 0;
        let mut deads = 0;
        for e in handle.ctx.stats.iter() {
            lives += e.live_keys;
            deads += e.dead_keys;
        }
        assert_eq!(10000, lives);
        assert_eq!(5000, deads);
    }

    #[test]
    fn bitcask_collect_statistics() {
        let dir = tempfile::tempdir().unwrap();
        // create lots of small files to test reading across different files
        let conf = Config::default()
            .concurrency(1)
            .max_file_size(ByteSize::kib(64))
            .to_owned();
        let kv = conf.open(dir.path()).unwrap();
        let handle = kv.get_handle();
        // put 10000 different keys
        for i in 0..10000 {
            handle
                .put(
                    Bytes::from(format!("key{}", i)),
                    Bytes::from(format!("value{}", i)),
                )
                .unwrap();
        }
        // should get 10000 live keys and 0 dead keys.
        let mut lives = 0;
        let mut deads = 0;
        for e in handle.ctx.stats.iter() {
            lives += e.live_keys;
            deads += e.dead_keys;
        }
        assert_eq!(10000, lives);
        assert_eq!(0, deads);

        // overwrite 5000 keys
        for i in 0..5000 {
            handle
                .put(
                    Bytes::from(format!("key{}", i)),
                    Bytes::from(format!("value{}", i)),
                )
                .unwrap();
        }
        // should get 10000 live keys and 5000 dead keys.
        let mut lives = 0;
        let mut deads = 0;
        for e in handle.ctx.stats.iter() {
            lives += e.live_keys;
            deads += e.dead_keys;
        }
        assert_eq!(10000, lives);
        assert_eq!(5000, deads);
    }
}
