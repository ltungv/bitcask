use bytes::Bytes;
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::{map::Entry, SkipMap};

use super::Config;

/// The context holds states that are shared across both reads and writes operations.
#[derive(Debug)]
pub(super) struct Context {
    /// The mapping from keys to the positions of their values on disk.
    keydir: SkipMap<Bytes, KeyDirEntry>,

    /// Mark whether the storage has been closed
    closed: AtomicCell<bool>,

    /// Storage configurations.
    conf: Config,
}

impl Context {
    /// Create a new Context for holding shared Bitcask states.
    pub(super) fn new(conf: Config, keydir: SkipMap<Bytes, KeyDirEntry>) -> Self {
        Self {
            conf,
            keydir,
            closed: AtomicCell::new(false),
        }
    }

    /// Set the keydir and returns the previous set entry if there's any.
    pub(super) fn keydir_set(
        &self,
        key: Bytes,
        keydir_entry: KeyDirEntry,
    ) -> Option<Entry<'_, Bytes, KeyDirEntry>> {
        let prev_entry = self.keydir.get(&key);
        self.keydir.insert(key, keydir_entry);
        prev_entry
    }

    /// Get a reference to the keydir.
    pub(super) fn get_keydir(&self) -> &SkipMap<Bytes, KeyDirEntry> {
        &self.keydir
    }

    /// Return true if the Bitcask instance has been closed.
    pub(super) fn is_closed(&self) -> bool {
        self.closed.load()
    }

    /// Close the current Bitcask instance.
    pub(super) fn close(&self) {
        self.closed.store(true)
    }

    /// Get the Bitcask instance configurations.
    pub(super) fn get_conf(&self) -> &Config {
        &self.conf
    }
}

/// A structure for the keydir entry pointing the position of the entry on the data file.
#[derive(Debug)]
pub(super) struct KeyDirEntry {
    pub(super) fileid: u64,
    pub(super) len: u64,
    pub(super) pos: u64,
    pub(super) tstamp: i64,
}
