//! An implementation of [Bitcask](https://riak.com/assets/bitcask-intro.pdf).

use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap},
    fs,
    io::{self, BufWriter},
    path::Path,
    sync::Arc,
};

use bytes::Bytes;
use chrono::Timelike;
use tracing::{debug, error};

use crate::storage::bitcask::{config::MergePolicy, log, HintFileEntry};

use super::{
    log::{LogDir, LogStatistics, LogWriter},
    utils::{self, datafile_name},
    Context, DataFileEntry, Error, KeyDirEntry, SyncStrategy,
};

/// The writer appends log entries to data files and ensures that indices in KeyDir point to a valid
/// file locations.
#[derive(Debug)]
pub(super) struct Writer {
    /// The states that are shared with the readers.
    ctx: Arc<Context>,

    /// The cache of file descriptors for reading the data files.
    readers: RefCell<LogDir>,

    /// A writer that appends entries to the currently active file.
    writer: LogWriter,

    /// Counts of different metrics about the storage.
    stats: HashMap<u64, LogStatistics>,

    /// The ID of the currently active file.
    active_fileid: u64,

    /// The number of bytes that have been written to the currently active file.
    written_bytes: u64,
}

impl Writer {
    /// Create a new `Writer` for writing Bitcask states.
    pub(super) fn new(
        ctx: Arc<Context>,
        readers: RefCell<LogDir>,
        writer: LogWriter,
        stats: HashMap<u64, LogStatistics>,
        active_fileid: u64,
        written_bytes: u64,
    ) -> Self {
        Self {
            ctx,
            readers,
            writer,
            stats,
            active_fileid,
            written_bytes,
        }
    }
    /// Set the value of a key and overwrite any existing value at that key.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    pub(super) fn put(&mut self, key: Bytes, value: Bytes) -> Result<(), Error> {
        // Write to disk
        let keydir_entry = self.write(utils::timestamp(), key.clone(), Some(value))?;
        // If we overwrite an existing value, update the storage statistics
        if let Some(prev_entry) = self.ctx.keydir_set(key, keydir_entry) {
            self.stats
                .entry(prev_entry.value().fileid)
                .or_default()
                .overwrite(prev_entry.value().len);
        }
        Ok(())
    }

    /// Delete a key and return `true`, if it exists. Otherwise, return `false`.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    pub(super) fn delete(&mut self, key: Bytes) -> Result<bool, Error> {
        // Write to disk
        self.write(utils::timestamp(), key.clone(), None)?;
        // If we overwrite an existing value, update the storage statistics
        match self.ctx.get_keydir().remove(&key) {
            Some(prev_entry) => {
                self.stats
                    .entry(prev_entry.value().fileid)
                    .or_default()
                    .overwrite(prev_entry.value().len);
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
        let conf = self.ctx.get_conf();
        if let SyncStrategy::Always = conf.sync {
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
            let entry = self.stats.entry(self.active_fileid).or_default();
            if datafile_entry.value.is_some() {
                entry.add_live();
            } else {
                entry.add_dead(index.len);
            }
            debug!(
                entry_len = %index.len,
                entry_pos = %index.pos,
                active_fileid = %self.active_fileid,
                active_file_size = %self.written_bytes,
                active_live_keys = %entry.live_keys(),
                active_dead_keys = %entry.dead_keys(),
                active_dead_bytes = %entry.dead_bytes(),
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
        if self.written_bytes > conf.max_file_size.get() {
            self.new_active_datafile(self.active_fileid + 1)?;
        }
        Ok(keydir_entry)
    }

    /// Copy data from files that are included for merging. Once finish, copied files are deleted.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) fn merge(&mut self) -> Result<(), Error> {
        let conf = self.ctx.get_conf();
        let path = conf.path.as_path();
        let min_merge_fileid = self.active_fileid + 1;
        let mut merge_fileid = min_merge_fileid;
        debug!(merge_fileid, "new merge file");

        // Get the set of file ids to be merged
        let fileids_to_merge = self.fileids_to_merge(path)?;
        // Copy entries to a temporary map so we don't modify the KeyDir while iterating.
        let mut new_keydir_entries = HashMap::new();

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
            for entry in self
                .ctx
                .get_keydir()
                .iter()
                .filter(|e| fileids_to_merge.contains(&e.value().fileid))
            {
                // SAFETY: We ensure in `BitcaskWriter` that all log entries given by
                // KeyDir are written disk, thus the readers can savely use memmap to
                // access the data file randomly.
                let nbytes = unsafe {
                    readers.copy(
                        path,
                        entry.value().fileid,
                        entry.value().len,
                        entry.value().pos,
                        &mut merge_datafile_writer,
                    )?
                };

                new_keydir_entries.insert(
                    entry.key().clone(),
                    KeyDirEntry {
                        fileid: merge_fileid,
                        len: nbytes,
                        pos: merge_pos,
                        tstamp: entry.value().tstamp,
                    },
                );

                // the merge file must only contain live keys
                let stats = self.stats.entry(merge_fileid).or_default();
                stats.add_live();

                // write the KeyDir entry to the hint file for fast recovery
                merge_hintfile_writer.append(&HintFileEntry {
                    tstamp: entry.value().tstamp,
                    len: entry.value().len,
                    pos: entry.value().pos,
                    key: entry.key().clone(),
                })?;

                // switch to new merge data file if we exceed the max file size
                merge_pos += nbytes;
                if merge_pos > conf.max_file_size.get() {
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

        // Update keydir so it points to the merge data file
        for (k, v) in new_keydir_entries {
            self.ctx.keydir_set(k, v);
        }

        // Remove stale files from system and storage statistics
        for id in &fileids_to_merge {
            self.stats.remove(id);
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

    /// Return `true` if one of the merge trigger conditions is met.
    pub(super) fn can_merge(&self) -> bool {
        let conf = self.ctx.get_conf();
        match conf.merge.policy {
            MergePolicy::Never => false,
            ref policy => {
                if let &MergePolicy::Window { start, end } = policy {
                    let now = chrono::Local::now().time();
                    let hour = now.hour();
                    if hour < start || hour > end {
                        return false;
                    }
                }
                for (_, entry) in self.stats.iter() {
                    // If any file met one of the trigger conditions, we'll try to merge
                    if entry.dead_bytes() > conf.merge.triggers.dead_bytes
                        || entry.fragmentation() > conf.merge.triggers.fragmentation
                    {
                        return true;
                    }
                }
                false
            }
        }
    }

    /// Synchronize data to disk. This tells the operating system to flush its internal buffer to
    /// ensure that data is actually persisted.
    pub(super) fn sync(&mut self) -> Result<(), Error> {
        self.writer.sync()?;
        Ok(())
    }

    /// Return the HashMap containing the writer statistics.
    #[cfg(test)]
    pub(super) fn get_stats(&self) -> &HashMap<u64, LogStatistics> {
        &self.stats
    }

    /// Updates the active file ID and open a new data file with the new active ID.
    #[tracing::instrument(level = "debug", skip(self))]
    fn new_active_datafile(&mut self, fileid: u64) -> Result<(), Error> {
        let conf = self.ctx.get_conf();
        self.active_fileid = fileid;
        self.writer = LogWriter::new(log::create(utils::datafile_name(
            conf.path.as_path(),
            self.active_fileid,
        ))?)?;
        self.written_bytes = 0;
        Ok(())
    }

    /// Return the set of file IDs that are included for merging.
    fn fileids_to_merge<P>(&self, path: P) -> Result<BTreeSet<u64>, Error>
    where
        P: AsRef<Path>,
    {
        let mut fileids = BTreeSet::new();
        for (&fileid, stats) in self.stats.iter() {
            let metadata = fs::metadata(datafile_name(&path, fileid))?;
            // Files that met one of the threshold conditions are included
            let conf = self.ctx.get_conf();
            if stats.dead_bytes() > conf.merge.thresholds.dead_bytes
                || stats.fragmentation() > conf.merge.thresholds.fragmentation
                || metadata.len() < conf.merge.thresholds.small_file
            {
                fileids.insert(fileid);
            }
        }
        Ok(fileids)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if self.written_bytes != 0 {
            return;
        }
        let conf = self.ctx.get_conf();
        let active_datafile = utils::datafile_name(&conf.path, self.active_fileid);
        if let Err(e) = fs::remove_file(active_datafile) {
            error!(cause=?e, fileid=self.active_fileid, "can't remove empty data file");
        }
    }
}
