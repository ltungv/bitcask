//! An implementation of [Bitcask](https://riak.com/assets/bitcask-intro.pdf).

mod bufio;
mod config;
mod log;
mod utils;

use std::{
    cell::RefCell,
    collections::BTreeSet,
    fs,
    io::{self, BufWriter},
    path::Path,
    sync::Arc,
    time,
};

use bytes::Bytes;
use chrono::Timelike;
use crossbeam::{atomic::AtomicCell, queue::ArrayQueue, utils::Backoff};
use dashmap::DashMap;
use parking_lot::Mutex;
use rand::prelude::Distribution;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{join, sync::broadcast};
use tracing::{debug, error, info};

pub use self::config::{Config, SyncStrategy};
use self::{
    log::{LogDir, LogIterator, LogStatistics, LogWriter},
    utils::datafile_name,
};
use super::KeyValueStorage;
use crate::{shutdown::Shutdown, storage::bitcask::config::MergePolicy};

/// Error returned by Bitcask
#[derive(Error, Debug)]
pub enum Error {
    /// Error from operating on a closed storage
    #[error("Storage has been closed")]
    Closed,

    /// Error from I/O operations.
    #[error("I/O error - {0}")]
    Io(#[from] io::Error),

    /// Error from serialization and deserialization.
    #[error("Serialization error - {0}")]
    Serialization(#[from] bincode::Error),

    /// Error from running asynchronous tasks.
    #[error("Asynchronous task error - {0}")]
    AsyncTask(#[from] tokio::task::JoinError),
}

/// An implementation of a Bitcask instance whose APIs resemble the one given in [bitcask-intro.pdf]
/// but with a few methods omitted.
///
/// Each Bitcask instance is a directory containing data files. At any moment, one file is "active"
/// for writing, and Bitcask sequentially appends data to the active data file. Bitcask keeps a
/// KeyDir that maps a key to the position of its value in the data files and uses the KeyDir to
/// access the data file entries directly without having to scan all data files.
///
/// Operations on the Bitcask instance are not directly handled by this struct. Instead, it gives
/// out handles to the Bitcask instance to threads that need it, and operations on the instance
/// are concurrently executed through these handles. The lifetime of this struct is tied to the
/// lifetimeof the storage. When this struct is dropped, it sets the shutdown state of the storage
/// and notifies all background tasks about the change so they can gracefully stop.
///
/// [bitcask-intro.pdf]: https://riak.com/assets/bitcask-intro.pdf
pub struct Bitcask {
    /// The handle to the Bitcask instance.
    handle: Handle,

    /// A channel for broadcasting shutdown signal so background tasks can gracefully stop. Tasks
    /// that want to check if the storage has been shutted down subscribe to this channel and wait
    /// for the signal that is sent when this struct is dropped. We do not send messages directly
    /// through the channel but rely on it's `Drop` implementation to send a closing signal.
    notify_shutdown: broadcast::Sender<()>,
}

/// A handle that can be shared across threads that want to access the storage.
#[derive(Clone, Debug)]
pub struct Handle {
    /// The states that are shared between the writer and the readers.
    ctx: Arc<Context>,

    /// A mutex-protected writer used for appending data entry to the active data files. All
    /// operations that make changes to the active data file are delegated to this object.
    writer: Arc<Mutex<Writer>>,

    /// A readers queue for parallelizing read-access to the key-value store. Upon a read-access,
    /// a reader is taken from the queue and used for reading the data files. Once we finish
    /// reading, the reader is returned back to the queue.
    readers: Arc<ArrayQueue<Reader>>,
}

/// The context holds states that are shared across both reads and writes operations.
#[derive(Debug)]
struct Context {
    /// Storage configurations.
    conf: Config,

    /// The mapping from keys to the positions of their values on disk.
    keydir: DashMap<Bytes, KeyDirEntry>,

    /// Counts of different metrics about the storage.
    stats: DashMap<u64, LogStatistics>,

    /// Mark whether the storage has been closed
    closed: AtomicCell<bool>,
}

/// The writer appends log entries to data files and ensures that indices in KeyDir point to a valid
/// file locations.
#[derive(Debug)]
struct Writer {
    /// The states that are shared with the readers.
    ctx: Arc<Context>,

    /// The cache of file descriptors for reading the data files.
    readers: RefCell<LogDir>,

    /// A writer that appends entries to the currently active file.
    writer: LogWriter,

    /// The ID of the currently active file.
    active_fileid: u64,

    /// The number of bytes that have been written to the currently active file.
    written_bytes: u64,
}

/// The reader reads log entries from data files given the locations found in KeyDir. Since data files
/// are immutable (except for the active one), we can safely read them concurrently without any extra
/// synchronization between threads.
#[derive(Debug)]
struct Reader {
    /// The states that are shared with the writer.
    ctx: Arc<Context>,

    /// The cache of file descriptors for reading the data files.
    readers: RefCell<LogDir>,
}

impl Bitcask {
    fn open(conf: Config) -> Result<Self, Error> {
        info!(?conf, "openning bitcask");

        // Reconstruct in-memory data from on-disk data
        let (keydir, stats, active_fileid) = rebuild_storage(&conf.path)?;
        debug!(?active_fileid, "got new active file ID");

        let ctx = Arc::new(Context {
            conf,
            keydir,
            stats,
            closed: AtomicCell::new(false),
        });

        let readers = Arc::new(ArrayQueue::new(ctx.conf.concurrency.get()));
        for _ in 0..readers.capacity() {
            readers
                .push(Reader {
                    ctx: ctx.clone(),
                    readers: RefCell::new(LogDir::new(ctx.conf.readers_cache_size)),
                })
                .expect("unreachable error");
        }

        let writer = Arc::new(Mutex::new(Writer {
            ctx: ctx.clone(),
            readers: RefCell::new(LogDir::new(ctx.conf.readers_cache_size)),
            writer: LogWriter::new(log::create(utils::datafile_name(
                &ctx.conf.path,
                active_fileid,
            ))?)?,
            active_fileid,
            written_bytes: 0,
        }));

        let handle = Handle {
            ctx,
            writer,
            readers,
        };

        // We'll tie the lifetime of this channel to the lifetime of our `Bitcask` struct so
        // the channel is closed when the struct is dropped
        let (notify_shutdown, _) = broadcast::channel(1);
        let bitcask = Self {
            handle,
            notify_shutdown,
        };

        // We spawn a dedicated thread for the background task. The thread will host a
        // Tokio runtime to schedule tasks for execution.
        let handle = bitcask.get_handle();
        let notify_shutdown = bitcask.notify_shutdown.clone();
        std::thread::Builder::new()
            .name("bitcask-background-tasks".into())
            .spawn(move || background_tasks(handle, notify_shutdown))?;

        Ok(bitcask)
    }

    /// Get the handle to the storage
    pub fn get_handle(&self) -> Handle {
        self.handle.clone()
    }
}

impl Drop for Bitcask {
    fn drop(&mut self) {
        self.handle.close();
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
    fn put(&self, key: Bytes, value: Bytes) -> Result<(), Error> {
        if self.ctx.closed.load() {
            return Err(Error::Closed);
        }
        self.writer.lock().put(key, value)
    }

    fn delete(&self, key: Bytes) -> Result<bool, Error> {
        if self.ctx.closed.load() {
            return Err(Error::Closed);
        }
        self.writer.lock().delete(key)
    }

    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Error> {
        if self.ctx.closed.load() {
            return Err(Error::Closed);
        }
        let backoff = Backoff::new();
        loop {
            if let Some(reader) = self.readers.pop() {
                // Make a query with the key and return the context to the queue after we finish so
                // other threads can make progress
                let result = reader.get(key);
                self.readers.push(reader).expect("unreachable error");
                break result;
            }
            // Spin until we have access to a reader
            backoff.spin();
        }
    }

    fn merge(&self) -> Result<(), Error> {
        if self.ctx.closed.load() {
            return Err(Error::Closed);
        }
        self.writer.lock().merge()
    }

    fn sync(&self) -> Result<(), Error> {
        if self.ctx.closed.load() {
            return Err(Error::Closed);
        }
        self.writer.lock().sync()
    }

    fn close(&self) {
        self.ctx.closed.store(true)
    }
}

impl Context {
    /// Return `true` if one of the merge trigger conditions is met.
    fn can_merge(&self) -> bool {
        match self.conf.merge.policy {
            MergePolicy::Never => false,
            ref policy => {
                if let &MergePolicy::Window { start, end } = policy {
                    let now = chrono::Local::now().time();
                    let hour = now.hour();
                    if hour < start || hour > end {
                        return false;
                    }
                }
                for entry in self.stats.iter() {
                    // If any file met one of the trigger conditions, we'll try to merge
                    if entry.dead_bytes > self.conf.merge.triggers.dead_bytes
                        || entry.fragmentation() > self.conf.merge.triggers.fragmentation
                    {
                        return true;
                    }
                }
                false
            }
        }
    }

    /// Return the set of file IDs that are included for merging.
    fn fileids_to_merge<P>(&self, path: P) -> Result<BTreeSet<u64>, Error>
    where
        P: AsRef<Path>,
    {
        let mut fileids = BTreeSet::new();
        for entry in self.stats.iter() {
            let fileid = *entry.key();
            let metadata = fs::metadata(datafile_name(&path, fileid))?;
            // Files that met one of the threshold conditions are included
            if entry.dead_bytes > self.conf.merge.thresholds.dead_bytes
                || entry.fragmentation() > self.conf.merge.thresholds.fragmentation
                || metadata.len() < self.conf.merge.thresholds.small_file
            {
                fileids.insert(fileid);
            }
        }
        Ok(fileids)
    }
}

impl Writer {
    /// Set the value of a key and overwrite any existing value at that key.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn put(&mut self, key: Bytes, value: Bytes) -> Result<(), Error> {
        // Write to disk
        let keydir_entry = self.write(utils::timestamp(), key.clone(), Some(value))?;
        // If we overwrite an existing value, update the storage statistics
        if let Some(prev_keydir_entry) = self.ctx.keydir.insert(key, keydir_entry) {
            self.ctx
                .stats
                .entry(prev_keydir_entry.fileid)
                .or_default()
                .overwrite(prev_keydir_entry.len);
        }
        Ok(())
    }

    /// Delete a key and return `true`, if it exists. Otherwise, return `false`.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn delete(&mut self, key: Bytes) -> Result<bool, Error> {
        // Write to disk
        self.write(utils::timestamp(), key.clone(), None)?;
        // If we overwrite an existing value, update the storage statistics
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
        tstamp: i64,
        key: Bytes,
        value: Option<Bytes>,
    ) -> Result<KeyDirEntry, Error> {
        // Append log entry
        let datafile_entry = DataFileEntry { tstamp, key, value };
        let index = self.writer.append(&datafile_entry)?;
        // Sync immediately if the strategy is "always"
        if let SyncStrategy::Always = self.ctx.conf.sync {
            self.writer.sync()?;
        }
        // Record number of bytes have been written to the active file
        self.written_bytes += index.len;

        // NOTE: This explicit scope is used to control the lifetime of `stats` which we borrow
        // from `self`. `stats` has to be dropped before we make a call to `new_active_datafile`.
        {
            // Collect statistics of the active data file for the merging process. If we add
            // a value to a key, we increase the number of live keys. If we add a tombstone,
            // we increase the number of dead keys.
            let mut stats = self.ctx.stats.entry(self.active_fileid).or_default();
            if datafile_entry.value.is_some() {
                stats.add_live();
            } else {
                stats.add_dead(index.len);
            }
            debug!(
                entry_len = %index.len,
                entry_pos = %index.pos,
                active_fileid = %self.active_fileid,
                active_file_size = %self.written_bytes,
                active_live_keys = %stats.live_keys,
                active_dead_keys = %stats.dead_keys,
                active_dead_bytes = %stats.dead_bytes,
                "appended new log entry"
            );
        }

        let keydir_entry = KeyDirEntry {
            fileid: self.active_fileid,
            len: index.len,
            pos: index.pos,
            tstamp,
        };

        // Check if active file size exceeds the max limit. This must be done as the last step of
        // the writing process, otherwise we risk corrupting the storage states.
        if self.written_bytes > self.ctx.conf.max_file_size.get() {
            self.new_active_datafile(self.active_fileid + 1)?;
        }
        Ok(keydir_entry)
    }

    /// Copy data from files that are included for merging. Once finish, copied files are deleted.
    #[tracing::instrument(level = "debug", skip(self))]
    fn merge(&mut self) -> Result<(), Error> {
        let path = self.ctx.conf.path.as_path();
        let min_merge_fileid = self.active_fileid + 1;
        let mut merge_fileid = min_merge_fileid;
        debug!(merge_fileid, "new merge file");

        // Get the set of file ids to be merged
        let fileids_to_merge = self.ctx.fileids_to_merge(path)?;

        // NOTE: we use an explicit scope here to control the lifetimes of `readers`,
        // `merge_datafile_writer` and `merge_hintfile_writer`. We drop the readers
        // early so we can later mutably borrow `self` and drop the writers early so
        // they are flushed.
        {
            let mut readers = self.readers.borrow_mut();
            let mut merge_pos = 0;
            let mut merge_datafile_writer =
                BufWriter::new(log::create(utils::datafile_name(path, merge_fileid))?);
            let mut merge_hintfile_writer =
                LogWriter::new(log::create(utils::hintfile_name(path, merge_fileid))?)?;

            // Only go through entries whose values are located within the merged files.
            for mut keydir_entry in self
                .ctx
                .keydir
                .iter_mut()
                .filter(|e| fileids_to_merge.contains(&e.fileid))
            {
                // SAFETY: We ensure in `BitcaskWriter` that all log entries given by
                // KeyDir are written disk, thus the readers can savely use memmap to
                // access the data file randomly.
                let nbytes = unsafe {
                    readers.copy(
                        path,
                        keydir_entry.fileid,
                        keydir_entry.len,
                        keydir_entry.pos,
                        &mut merge_datafile_writer,
                    )?
                };

                // update keydir so it points to the merge data file
                keydir_entry.fileid = merge_fileid;
                keydir_entry.len = nbytes;
                keydir_entry.pos = merge_pos;

                // the merge file must only contain live keys
                let mut stats = self.ctx.stats.entry(merge_fileid).or_default();
                stats.add_live();

                // write the KeyDir entry to the hint file for fast recovery
                merge_hintfile_writer.append(&HintFileEntry {
                    tstamp: keydir_entry.tstamp,
                    len: keydir_entry.len,
                    pos: keydir_entry.pos,
                    key: keydir_entry.key().clone(),
                })?;

                // switch to new merge data file if we exceed the max file size
                merge_pos += nbytes;
                if merge_pos > self.ctx.conf.max_file_size.get() {
                    merge_fileid += 1;
                    merge_pos = 0;
                    merge_datafile_writer =
                        BufWriter::new(log::create(utils::datafile_name(path, merge_fileid))?);
                    merge_hintfile_writer =
                        LogWriter::new(log::create(utils::hintfile_name(path, merge_fileid))?)?;
                    debug!(merge_fileid, "new merge file");
                }
            }
        }

        // Remove stale files from system and storage statistics
        for id in &fileids_to_merge {
            self.ctx.stats.remove(id);
            if let Err(e) = fs::remove_file(utils::hintfile_name(path, *id)) {
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(e.into());
                }
            }
            if let Err(e) = fs::remove_file(utils::datafile_name(path, *id)) {
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(e.into());
                }
            }
        }

        self.new_active_datafile(merge_fileid + 1)?;
        Ok(())
    }

    /// Updates the active file ID and open a new data file with the new active ID.
    #[tracing::instrument(level = "debug", skip(self))]
    fn new_active_datafile(&mut self, fileid: u64) -> Result<(), Error> {
        self.active_fileid = fileid;
        self.writer = LogWriter::new(log::create(utils::datafile_name(
            self.ctx.conf.path.as_path(),
            self.active_fileid,
        ))?)?;
        self.written_bytes = 0;
        Ok(())
    }

    /// Synchronize data to disk. This tells the operating system to flush its internal buffer to
    /// ensure that data is actually persisted.
    pub fn sync(&mut self) -> Result<(), Error> {
        self.writer.sync()?;
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
                // SAFETY: We have taken `keydir_entry` from KeyDir which is ensured to point to
                // valid data file positions. Thus we can be confident that the Mmap won't be
                // mapped to an invalid segment.
                let datafile_entry = unsafe {
                    self.readers.borrow_mut().read::<DataFileEntry, _>(
                        &self.ctx.conf.path,
                        keydir_entry.fileid,
                        keydir_entry.len,
                        keydir_entry.pos,
                    )?
                };

                Ok(datafile_entry.value)
            }
            None => Ok(None),
        }
    }
}

#[tracing::instrument(skip(handle, notify_shutdown))]
fn background_tasks(handle: Handle, notify_shutdown: broadcast::Sender<()>) -> Result<(), Error> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let merge_join_handle = {
        let handle = handle.clone();
        let shutdown = Shutdown::new(notify_shutdown.subscribe());
        rt.spawn(async move {
            if let Err(e) = merge_on_interval(handle, shutdown).await {
                error!(cause=?e, "merge error");
            }
        })
    };
    let sync_join_handle = {
        let handle = handle.clone();
        let shutdown = Shutdown::new(notify_shutdown.subscribe());
        rt.spawn(async move {
            if let Err(e) = sync_on_interval(handle, shutdown).await {
                error!(cause=?e, "sync error");
            }
        })
    };

    // Drop unused handle
    drop(handle);
    // We drop this early so there's only 1 channel Sender held by our bitcask instance
    drop(notify_shutdown);
    // Block until the async tasks finish
    let (r1, r2) = rt.block_on(async { join!(merge_join_handle, sync_join_handle) });
    if let Err(e) = r1 {
        error!(cause=?e, "merge error");
    }
    if let Err(e) = r2 {
        error!(cause=?e, "sync error");
    }
    Ok(())
}

/// A periodic background task that checks the merge triggers and performs merging when the trigger
/// conditions are met.
#[tracing::instrument(skip(handle, shutdown))]
async fn merge_on_interval(handle: Handle, mut shutdown: Shutdown) -> Result<(), Error> {
    // Return early so we don't have to run the task
    if let MergePolicy::Never = handle.ctx.conf.merge.policy {
        return Ok(());
    }
    let interval = time::Duration::from_millis(handle.ctx.conf.merge.check_interval_ms);
    let jitter = interval.mul_f64(handle.ctx.conf.merge.check_jitter);
    let dist = rand::distributions::Uniform::new_inclusive(interval - jitter, interval + jitter);
    while !shutdown.is_shutdown() {
        // Wake up the task when a specific interval has passed or when the storage is shutdown.
        tokio::select! {
            _ = tokio::time::sleep(dist.sample(&mut rand::thread_rng())) => {},
            _ = shutdown.recv() => {
                info!("stopping merge background task");
                return Ok(());
            },
        };
        if handle.ctx.can_merge() {
            let handle = handle.clone();
            if let Err(e) = tokio::task::spawn_blocking(move || handle.merge()).await? {
                error!(cause=?e, "merge error");
            }
        }
    }
    Ok(())
}

/// A periodic background task that forces disk synchronizations.
#[tracing::instrument(skip(handle, shutdown))]
async fn sync_on_interval(handle: Handle, mut shutdown: Shutdown) -> Result<(), Error> {
    // Only run task if we are requested to periodically sync
    if let SyncStrategy::IntervalMs(d) = handle.ctx.conf.sync {
        let interval = time::Duration::from_millis(d);
        while !shutdown.is_shutdown() {
            // Wake up the task when a specific interval has passed or when the storage is shutdown.
            tokio::select! {
                _ = tokio::time::sleep(interval) => {},
                _ = shutdown.recv() => {
                    info!("stopping sync background task");
                    return Ok(());
                },
            };
            let handle = handle.clone();
            if let Err(e) = tokio::task::spawn_blocking(move || handle.sync()).await? {
                error!(cause=?e, "sync error");
            }
        }
    }
    Ok(())
}

/// Read the given directory, rebuild the KeyDir, and gather statistics about the Bitcask instance
/// at that directory.
#[allow(clippy::type_complexity)]
fn rebuild_storage<P>(
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
        // Collect the most recent file id.
        match &mut active_fileid {
            None => active_fileid = Some(fileid),
            Some(id) => {
                if fileid > *id {
                    *id = fileid;
                }
            }
        }
        // Read the hint file, if it does not exist, read the data file.
        if let Err(e) = populate_keydir_with_hintfile(&path, fileid, &keydir, &stats) {
            match e {
                Error::Io(ref ioe) => match ioe.kind() {
                    io::ErrorKind::NotFound => {
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

/// Read the hint file with `fileid` in `path` and populate the given maps.
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
        // Hint file always contains live keys
        stats.entry(fileid).or_default().add_live();
        // Overwrite previously written value
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
            // Tombstone
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
                // Add live keys
                stats.entry(fileid).or_default().add_live();
                // Overwrite previous value
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

#[derive(Debug)]
struct KeyDirEntry {
    fileid: u64,
    len: u64,
    pos: u64,
    tstamp: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct HintFileEntry {
    tstamp: i64,
    len: u64,
    pos: u64,
    key: Bytes,
}

#[derive(Serialize, Deserialize, Debug)]
struct DataFileEntry {
    tstamp: i64,
    key: Bytes,
    value: Option<Bytes>,
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use proptest::{collection, prelude::*};

    use super::*;

    #[test]
    fn bitcask_sequential_read_after_write_should_return_the_written_data() {
        let dir = tempfile::tempdir().unwrap();
        let conf = Config::default()
            .concurrency(NonZeroUsize::new(1).unwrap())
            .path(dir.path())
            .to_owned();
        let kv = conf.open().unwrap();
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
            .concurrency(NonZeroUsize::new(1).unwrap())
            .max_file_size(NonZeroU64::new(64 * 1024).unwrap())
            .path(dir.path())
            .to_owned();
        {
            let kv = conf.clone().open().unwrap();
            let handle = kv.get_handle();
            // put 10000 different keys
            for i in 0..10000 {
                handle
                    .put(
                        Bytes::from(format!("key{i}")),
                        Bytes::from(format!("value{i}")),
                    )
                    .unwrap();
            }
        }

        // rebuild bitcask
        let kv = conf.open().unwrap();
        let handle = kv.get_handle();
        // get 10000 different keys
        for i in 0..10000 {
            let value = handle.get(Bytes::from(format!("key{i}"))).unwrap().unwrap();
            assert_eq!(Bytes::from(format!("value{i}")), value);
        }
    }

    #[test]
    fn bitcask_rebuilt_stats_correctly() {
        let dir = tempfile::tempdir().unwrap();
        // create lots of small files to test reading across different files
        let conf = Config::default()
            .concurrency(NonZeroUsize::new(1).unwrap())
            .max_file_size(NonZeroU64::new(64 * 1024).unwrap())
            .path(dir.path())
            .to_owned();

        {
            let kv = conf.clone().open().unwrap();
            let handle = kv.get_handle();
            // put 10000 different keys
            for i in 0..10000 {
                handle
                    .put(
                        Bytes::from(format!("key{i}")),
                        Bytes::from(format!("value{i}")),
                    )
                    .unwrap();
            }
            // overwrite 5000 keys
            for i in 0..5000 {
                handle
                    .put(
                        Bytes::from(format!("key{i}")),
                        Bytes::from(format!("value{i}")),
                    )
                    .unwrap();
            }
        }

        // rebuild bitcask
        let kv = conf.open().unwrap();
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
            .concurrency(NonZeroUsize::new(1).unwrap())
            .max_file_size(NonZeroU64::new(64 * 1024).unwrap())
            .path(dir.path())
            .to_owned();
        let kv = conf.open().unwrap();
        let handle = kv.get_handle();
        // put 10000 different keys
        for i in 0..10000 {
            handle
                .put(
                    Bytes::from(format!("key{i}")),
                    Bytes::from(format!("value{i}")),
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
                    Bytes::from(format!("key{i}")),
                    Bytes::from(format!("value{i}")),
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
