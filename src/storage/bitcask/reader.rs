use std::{cell::RefCell, sync::Arc};

use bytes::Bytes;

use super::{log::LogDir, Context, DataFileEntry, Error};

/// The reader reads log entries from data files given the locations found in KeyDir. Since data files
/// are immutable (except for the active one), we can safely read them concurrently without any extra
/// synchronization between threads.
#[derive(Debug)]
pub(super) struct Reader {
    /// The states that are shared with the writer.
    ctx: Arc<Context>,

    /// The cache of file descriptors for reading the data files.
    readers: RefCell<LogDir>,
}

impl Reader {
    /// Create a new `Reader` for reading Bitcask states.
    pub(super) fn new(ctx: Arc<Context>, readers: RefCell<LogDir>) -> Self {
        Self { ctx, readers }
    }

    /// Get the value of a key and return it, if it exists, otherwise return return `None`.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) fn get(&self, key: Bytes) -> Result<Option<Bytes>, Error> {
        match self.ctx.get_keydir().get(&key) {
            Some(keydir_entry) => {
                // SAFETY: We have taken `keydir_entry` from KeyDir which is ensured to point to
                // valid data file positions. Thus we can be confident that the Mmap won't be
                // mapped to an invalid segment.
                let datafile_entry = unsafe {
                    self.readers.borrow_mut().read::<DataFileEntry, _>(
                        &self.ctx.get_conf().path,
                        keydir_entry.value().fileid,
                        keydir_entry.value().len,
                        keydir_entry.value().pos,
                    )?
                };

                Ok(datafile_entry.value)
            }
            None => Ok(None),
        }
    }
}
