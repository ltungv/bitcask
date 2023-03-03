use std::{
    fs,
    io::{self, Write},
    num::NonZeroUsize,
    path::Path,
};

use bytes::Buf;
use lru::LruCache;
use serde::{de::DeserializeOwned, Serialize};

use super::{
    bufio::{BufReaderWithPos, BufWriterWithPos},
    utils, Error,
};

/// Position and length of an log entry within a log file.
#[derive(Debug, PartialEq, Eq)]
pub struct LogIndex {
    pub len: u64,
    pub pos: u64,
}

/// Keeping track of the number of live/dead keys and how much space do the dead keys occupy.
#[derive(Debug, Default)]
pub struct LogStatistics {
    live_keys: u64,
    dead_keys: u64,
    dead_bytes: u64,
}

impl LogStatistics {
    /// Add a live key to the statistics.
    pub fn add_live(&mut self) {
        self.live_keys += 1;
    }

    /// Add a dead key to the statistics where `nbytes` is the size of the entry on disk.
    pub fn add_dead(&mut self, nbytes: u64) {
        self.dead_keys += 1;
        self.dead_bytes += nbytes;
    }

    pub fn overwrite(&mut self, nbytes: u64) {
        self.live_keys -= 1;
        self.dead_keys += 1;
        self.dead_bytes += nbytes;
    }

    pub fn live_keys(&self) -> u64 {
        self.live_keys
    }

    pub fn dead_keys(&self) -> u64 {
        self.dead_keys
    }

    pub fn dead_bytes(&self) -> u64 {
        self.dead_bytes
    }

    /// Calculate the fraction of dead keys to total keys
    pub fn fragmentation(&self) -> f64 {
        // We avoid performing the calculation when there's no dead keys. This also helps avoiding
        // a division by zero
        let live = self.live_keys();
        let dead = self.dead_keys();
        if dead == 0 {
            return 0.0;
        }
        let total = dead + live;
        dead as f64 / total as f64
    }
}

/// A wrapper arround a LRU cache of log readers
#[derive(Debug)]
pub struct LogDir(LruCache<u64, LogReader>);

impl LogDir {
    /// Create a new LRU readers cache with the specified size.
    pub fn new(size: NonZeroUsize) -> Self {
        Self(LruCache::new(size))
    }

    pub unsafe fn read<T, P>(
        &mut self,
        path: P,
        fileid: u64,
        len: u64,
        pos: u64,
    ) -> Result<T, Error>
    where
        T: DeserializeOwned,
        P: AsRef<Path>,
    {
        match self.0.get_mut(&fileid) {
            Some(reader) => reader.at::<T>(len, pos),
            None => {
                let file = open(utils::datafile_name(&path, fileid))?;
                let mut reader = LogReader::new(file)?;
                let result = reader.at::<T>(len, pos);
                self.0.put(fileid, reader);
                result
            }
        }
    }

    pub unsafe fn copy<P, W>(
        &mut self,
        path: P,
        fileid: u64,
        len: u64,
        pos: u64,
        writer: &mut W,
    ) -> io::Result<u64>
    where
        P: AsRef<Path>,
        W: Write,
    {
        match self.0.get_mut(&fileid) {
            Some(reader) => reader.copy_raw(len, pos, writer),
            None => {
                let file = open(utils::datafile_name(&path, fileid))?;
                let mut reader = LogReader::new(file)?;
                let result = reader.copy_raw(len, pos, writer);
                self.0.put(fileid, reader);
                result
            }
        }
    }
}

/// An append-only file writer that serializes data using `bincode`.
#[derive(Debug)]
pub struct LogWriter(BufWriterWithPos<fs::File>);

impl LogWriter {
    /// Create a new log writer for writing entries to the given file.
    pub fn new(file: fs::File) -> io::Result<Self> {
        let writer = BufWriterWithPos::new(file)?;
        Ok(Self(writer))
    }

    /// Serialize the given entry at EOF and ensure to flush all data to the I/O device.
    pub fn append<T>(&mut self, entry: &T) -> Result<LogIndex, Error>
    where
        T: Serialize,
    {
        let pos = self.0.pos();

        bincode::serialize_into(&mut self.0, entry)?;
        self.0.flush()?;

        let len = self.0.pos() - pos;
        Ok(LogIndex { len, pos })
    }

    /// Synchronize all data to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.0.get_ref().sync_all()
    }
}

/// A random-access file reader that deserializes data using `bincode`.
#[derive(Debug)]
pub struct LogReader {
    mmap: memmap2::Mmap,
    file: fs::File,
}

impl LogReader {
    /// Create a new log reader for reading entries from the given file.
    pub fn new(file: fs::File) -> io::Result<Self> {
        // SAFETY: We just create a Mmap without doing any read so there's nothing to worry about.
        // All methods that access `LogReader` MUST be maked unsafe since the caller is responsible
        // for providing a valid file position.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        Ok(Self { mmap, file })
    }

    /// Return the entry at the given position by mapping the file segment directly into memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the file segment given by `len` and `pos` is valid.
    pub unsafe fn at<T>(&mut self, len: u64, pos: u64) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        // We assume that the caller always provide a valid data entry so we can expand the Mmap
        // and try reading with the `len` and `pos`.
        if pos >= self.mmap.len() as u64 {
            self.mmap = memmap2::MmapOptions::new().map(&self.file)?;
        }
        let start = pos as usize;
        let end = start + len as usize;
        Ok(bincode::deserialize(&self.mmap[(start..end)])?)
    }

    /// Copy the raw data at the given position into the writer at `dst` by mapping the file segment
    /// directly into memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the file segment given by `len` and `pos` is valid.
    pub unsafe fn copy_raw<W>(&mut self, len: u64, pos: u64, dst: &mut W) -> io::Result<u64>
    where
        W: Write,
    {
        // We assume that the caller always provide a valid data entry so we can expand the Mmap
        // and try reading with the `len` and `pos`.
        if pos >= self.mmap.len() as u64 {
            self.mmap = memmap2::MmapOptions::new().map(&self.file)?;
        }
        let start = pos as usize;
        let end = start + len as usize;
        io::copy(&mut self.mmap[start..end].reader(), dst)
    }
}

/// A sequential-access file reader that deserializes data using `bincode`.
#[derive(Debug)]
pub struct LogIterator(BufReaderWithPos<fs::File>);

impl LogIterator {
    /// Create a new log iterator for iterating through entries from the given file.
    pub fn new(file: fs::File) -> io::Result<Self> {
        let reader = BufReaderWithPos::new(file)?;
        Ok(Self(reader))
    }

    /// Return the entry at the current reader position.
    pub fn next<T>(&mut self) -> Result<Option<(LogIndex, T)>, Error>
    where
        T: DeserializeOwned,
    {
        // get reader current position so we can calculate the number of serialized bytes
        let pos = self.0.pos();
        match bincode::deserialize_from(&mut self.0) {
            Ok(entry) => {
                let len = self.0.pos() - pos;
                let index = LogIndex { len, pos };
                Ok(Some((index, entry)))
            }
            Err(e) => match e.as_ref() {
                bincode::ErrorKind::Io(ioe) => match ioe.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(None), // stop iterating when EOF
                    _ => Err(e.into()),
                },
                _ => Err(e.into()),
            },
        }
    }
}

/// Create a new data file for writing entries to.
pub fn create<P>(path: P) -> io::Result<fs::File>
where
    P: AsRef<Path>,
{
    fs::OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(path)
}

/// Open a data file for reading entries from.
pub fn open<P>(path: P) -> io::Result<fs::File>
where
    P: AsRef<Path>,
{
    fs::OpenOptions::new().read(true).open(path)
}

#[cfg(test)]
mod tests {
    use proptest::{collection::vec, prelude::*};

    use super::*;

    proptest! {
        #[test]
        fn writer_position_updated_after_write(buf in vec(any::<u8>(), 0..2048)) {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");
            // write the entry
            let mut writer = LogWriter::new(create(fpath).unwrap()).unwrap();
            let idx1 = writer.append(&buf).unwrap();
            let idx2 = writer.append(&buf).unwrap();
            // succeed if we received the correct index
            prop_assert_eq!(idx1.pos, 0);
            prop_assert_eq!(idx1.len, idx2.pos);
            prop_assert_eq!(idx1.len, idx2.len);
        }

        #[test]
        fn reader_reads_entry_written_by_writer(buf in vec(any::<u8>(), 0..2048)) {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");
            // write the entry
            let mut writer = LogWriter::new(create(&fpath).unwrap()).unwrap();
            let idx1 = writer.append(&buf).unwrap();
            let idx2 = writer.append(&buf).unwrap();
            // read the entry
            let mut reader = LogReader::new(open(&fpath).unwrap()).unwrap();
            let buf1 = unsafe { reader.at::<Vec<u8>>(idx1.len, idx1.pos).unwrap() };
            let buf2 = unsafe { reader.at::<Vec<u8>>(idx2.len, idx2.pos).unwrap() };
            // succeed if we received the correct data given the positions
            prop_assert_eq!(&buf, &buf1);
            prop_assert_eq!(&buf, &buf2);
        }

        #[test]
        fn reader_should_remap_disk_when_file_changed(buf in vec(any::<u8>(), 0..2048)) {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");
            let mut writer = LogWriter::new(create(&fpath).unwrap()).unwrap();
            let mut reader = LogReader::new(open(&fpath).unwrap()).unwrap();

            // write the entry
            let idx1 = writer.append(&buf).unwrap();
            let idx2 = writer.append(&buf).unwrap();
            // read the entry (should remap)
            let buf1 = unsafe { reader.at::<Vec<u8>>(idx1.len, idx1.pos).unwrap() };
            let buf2 = unsafe { reader.at::<Vec<u8>>(idx2.len, idx2.pos).unwrap() };

            // write some more
            let idx3 = writer.append(&buf).unwrap();
            let idx4 = writer.append(&buf).unwrap();
            // read the entry (should remap)
            let buf3 = unsafe { reader.at::<Vec<u8>>(idx3.len, idx3.pos).unwrap() };
            let buf4 = unsafe { reader.at::<Vec<u8>>(idx4.len, idx4.pos).unwrap() };

            // succeed if we received the correct data given the positions
            prop_assert_eq!(&buf, &buf1);
            prop_assert_eq!(&buf, &buf2);
            prop_assert_eq!(&buf, &buf3);
            prop_assert_eq!(&buf, &buf4);
        }

        #[test]
        fn reader_iterates_entries_written_by_writer(entries in vec(vec(any::<u8>(), 0..2048), 1..100)) {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");
            // write the entries
            let mut writer = LogWriter::new(create(&fpath).unwrap()).unwrap();
            let indices: Vec<LogIndex> = entries.iter().map(|buf| writer.append(&buf).unwrap()).collect();
            // read the entries
            let mut reader = LogReader::new(open(&fpath).unwrap()).unwrap();
            let mut iter = LogIterator::new(open(&fpath).unwrap()).unwrap();
            for (idx, buf) in indices.iter().zip(entries) {
                let (idx_from_reader, buf_from_reader) = iter.next::<Vec<u8>>().unwrap().unwrap();
                prop_assert_eq!(idx, &idx_from_reader);
                prop_assert_eq!(&buf, &buf_from_reader);
                let buf_from_reader = unsafe { reader.at::<Vec<u8>>(idx_from_reader.len, idx_from_reader.pos).unwrap() };
                prop_assert_eq!(&buf, &buf_from_reader);
            }
        }
    }
}
