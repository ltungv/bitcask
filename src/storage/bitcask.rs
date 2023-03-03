//! An implementation of [Bitcask](https://riak.com/assets/bitcask-intro.pdf).

mod bufio;
mod config;
mod context;
mod log;
mod reader;
mod utils;
mod writer;

use std::{cell::RefCell, collections::HashMap, io, path::Path, sync::Arc, time};

use bytes::Bytes;
use crossbeam::{queue::ArrayQueue, utils::Backoff};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use rand::prelude::Distribution;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{join, sync::broadcast};
use tracing::{debug, error, info};

pub use self::config::{Config, SyncStrategy};
use self::{
    context::KeyDirEntry,
    log::{LogDir, LogIterator, LogStatistics, LogWriter},
    reader::Reader,
    writer::Writer,
};
use super::KeyValueStorage;
use crate::{
    shutdown::Shutdown,
    storage::bitcask::{config::MergePolicy, context::Context},
};

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

impl Bitcask {
    fn open(conf: Config) -> Result<Self, Error> {
        info!(?conf, "openning bitcask");

        // Reconstruct in-memory data from on-disk data
        let (keydir, stats, active_fileid) = rebuild_storage(&conf.path)?;
        debug!(?active_fileid, "got new active file ID");

        let ctx = Arc::new(Context::new(conf, keydir));

        let readers = Arc::new(ArrayQueue::new(ctx.get_conf().concurrency.get()));
        for _ in 0..readers.capacity() {
            readers
                .push(Reader::new(
                    ctx.clone(),
                    RefCell::new(LogDir::new(ctx.get_conf().readers_cache_size)),
                ))
                .expect("unreachable error");
        }

        let writer = Arc::new(Mutex::new(Writer::new(
            ctx.clone(),
            RefCell::new(LogDir::new(ctx.get_conf().readers_cache_size)),
            LogWriter::new(log::create(utils::datafile_name(
                &ctx.get_conf().path,
                active_fileid,
            ))?)?,
            stats,
            active_fileid,
            0,
        )));

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

impl Handle {
    fn put(&self, key: Bytes, value: Bytes) -> Result<(), Error> {
        if self.ctx.is_closed() {
            return Err(Error::Closed);
        }
        self.writer.lock().put(key, value)
    }

    fn delete(&self, key: Bytes) -> Result<bool, Error> {
        if self.ctx.is_closed() {
            return Err(Error::Closed);
        }
        self.writer.lock().delete(key)
    }

    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Error> {
        if self.ctx.is_closed() {
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
        if self.ctx.is_closed() {
            return Err(Error::Closed);
        }
        let mut writer = self.writer.lock();
        if writer.can_merge() {
            writer.merge()?;
        }
        Ok(())
    }

    fn sync(&self) -> Result<(), Error> {
        if self.ctx.is_closed() {
            return Err(Error::Closed);
        }
        self.writer.lock().sync()
    }

    fn close(&self) {
        self.ctx.close()
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
    if let MergePolicy::Never = handle.ctx.get_conf().merge.policy {
        return Ok(());
    }
    let interval = time::Duration::from_millis(handle.ctx.get_conf().merge.check_interval_ms);
    let jitter = interval.mul_f64(handle.ctx.get_conf().merge.check_jitter);
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
        let handle = handle.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || handle.merge()).await? {
            error!(cause=?e, "merge error");
        }
    }
    Ok(())
}

/// A periodic background task that forces disk synchronizations.
#[tracing::instrument(skip(handle, shutdown))]
async fn sync_on_interval(handle: Handle, mut shutdown: Shutdown) -> Result<(), Error> {
    // Only run task if we are requested to periodically sync
    if let SyncStrategy::IntervalMs(d) = handle.ctx.get_conf().sync {
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
        SkipMap<Bytes, KeyDirEntry>,
        HashMap<u64, LogStatistics>,
        u64,
    ),
    Error,
>
where
    P: AsRef<Path>,
{
    let keydir = SkipMap::default();
    let mut stats = HashMap::default();
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
        if let Err(e) = populate_keydir_with_hintfile(&path, fileid, &keydir, &mut stats) {
            match e {
                Error::Io(ref ioe) => match ioe.kind() {
                    io::ErrorKind::NotFound => {
                        populate_keydir_with_datafile(&path, fileid, &keydir, &mut stats)?;
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
    keydir: &SkipMap<Bytes, KeyDirEntry>,
    stats: &mut HashMap<u64, LogStatistics>,
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
        let prev_entry = keydir.get(&entry.key);
        keydir.insert(entry.key, keydir_entry);
        if let Some(prev_entry) = prev_entry {
            stats
                .entry(prev_entry.value().fileid)
                .or_default()
                .overwrite(prev_entry.value().len);
        }
    }
    Ok(())
}

fn populate_keydir_with_datafile<P>(
    path: P,
    fileid: u64,
    keydir: &SkipMap<Bytes, KeyDirEntry>,
    stats: &mut HashMap<u64, LogStatistics>,
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
                let prev_entry = keydir.get(&datafile_entry.key);
                keydir.insert(datafile_entry.key, keydir_entry);
                if let Some(prev_entry) = prev_entry {
                    stats
                        .entry(prev_entry.value().fileid)
                        .or_default()
                        .overwrite(prev_entry.value().len);
                }
            }
        }
    }
    Ok(())
}

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
        for (_, e) in handle.writer.lock().get_stats().iter() {
            lives += e.live_keys();
            deads += e.dead_keys();
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
        for (_, e) in handle.writer.lock().get_stats().iter() {
            lives += e.live_keys();
            deads += e.dead_keys();
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
        for (_, e) in handle.writer.lock().get_stats().iter() {
            lives += e.live_keys();
            deads += e.dead_keys();
        }
        assert_eq!(10000, lives);
        assert_eq!(5000, deads);
    }
}
