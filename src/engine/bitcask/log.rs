use std::{
    fs,
    io::{self, Write},
    path::Path,
};

use bytes::Buf;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;

use super::{
    bufio::{BufReaderWithPos, BufWriterWithPos},
    utils,
};

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

/// A mapping of log file IDs to log file readers.
#[derive(Debug, Default)]
pub struct LogDir(BTreeMap<u64, LogReader>);

impl LogDir {
    /// Deserialize a data entry given the directory path and the file position
    pub fn read<T, P>(&mut self, path: P, fileid: u64, len: u64, pos: u64) -> bincode::Result<T>
    where
        T: DeserializeOwned,
        P: AsRef<Path>,
    {
        // NOTE: Unsafe usage.
        // We ensure in `BitCaskWriter` that all log entries given by KeyDir are written disk,
        // thus the readers can safely use memmap to access the data file randomly.
        let prev_datafile_entry = unsafe { self.get(path, fileid)?.at::<T>(len, pos)? };
        Ok(prev_datafile_entry)
    }

    /// Return the reader of the file with the given `fileid`. If there's no reader for the file
    /// with the given `fileid`, create a new reader and return it.
    pub fn get<P>(&mut self, path: P, fileid: u64) -> io::Result<&LogReader>
    where
        P: AsRef<Path>,
    {
        let reader = self
            .0
            .entry(fileid)
            .or_insert(LogReader::new(open(utils::datafile_name(&path, fileid))?));
        Ok(reader)
    }

    /// Remove readers whose ID is smaller than the given `min_fileid`.
    pub fn drop_stale(&mut self, min_fileid: u64) {
        let stale_fileids: Vec<u64> = self
            .0
            .keys()
            .filter(|&&id| id < min_fileid)
            .cloned()
            .collect();
        stale_fileids.iter().for_each(|id| {
            self.0.remove(id);
        });
    }
}

/// Position and length of an log entry within a log file.
#[derive(Debug, PartialEq, Eq)]
pub struct LogIndex {
    pub len: u64,
    pub pos: u64,
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
    pub fn append<T>(&mut self, entry: &T) -> bincode::Result<LogIndex>
    where
        T: Serialize,
    {
        let pos = self.0.pos();

        bincode::serialize_into(&mut self.0, entry)?;
        self.0.flush()?;

        let len = self.0.pos() - pos;
        Ok(LogIndex { len, pos })
    }

    /// Synchronize the writer state with the file system and ensure all data is physically written.
    pub fn sync_all(&mut self) -> io::Result<()> {
        self.0.get_ref().sync_all()
    }
}

/// A random-access file reader that deserializes data using `bincode`.
#[derive(Debug)]
pub struct LogReader(fs::File);

impl LogReader {
    /// Create a new log reader for reading entries from the given file.
    pub fn new(file: fs::File) -> Self {
        Self(file)
    }

    /// Return the entry at the given position by mapping the file segment directly into memory.
    ///
    /// # Unsafe
    ///
    /// The caller must ensure that the file segment given by `len` and `pos` is valid.
    pub unsafe fn at<T>(&self, len: u64, pos: u64) -> bincode::Result<T>
    where
        T: DeserializeOwned,
    {
        let mmap = memmap2::MmapOptions::new()
            .offset(pos)
            .len(len as usize)
            // NOTE: we convert an io::Error into bincode::Error which might be undesirable
            .map(&self.0)?;
        bincode::deserialize(&mmap)
    }

    /// Copy the raw data at the given position into the writer at `dst` by mapping the file segment
    /// directly into memory.
    ///
    /// # Unsafe
    ///
    /// The caller must ensure that the file segment given by `len` and `pos` is valid.
    pub unsafe fn copy_raw<W>(&self, len: u64, pos: u64, dst: &mut W) -> io::Result<u64>
    where
        W: Write,
    {
        let mmap = memmap2::MmapOptions::new()
            .offset(pos)
            .len(len as usize)
            .map(&self.0)?;
        io::copy(&mut mmap.reader(), dst)
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
    pub fn next<T>(&mut self) -> bincode::Result<Option<(LogIndex, T)>>
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
                    _ => Err(e),
                },
                _ => Err(e),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::quickcheck;

    use super::*;

    quickcheck! {
        fn writer_position_updated_after_write(buf: Vec<u8>) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");
            // write the entry
            let mut writer = LogWriter::new(create(&fpath).unwrap()).unwrap();
            let idx1 = writer.append(&buf).unwrap();
            let idx2 = writer.append(&buf).unwrap();
            // succeed if we received the correct index
            idx1.pos == 0 && idx1.len == idx2.pos && idx1.len == idx2.len
        }
    }

    quickcheck! {
        fn reader_reads_entry_written_by_writer(buf: Vec<u8>) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");
            // write the entry
            let mut writer = LogWriter::new(create(&fpath).unwrap()).unwrap();
            let idx1 = writer.append(&buf).unwrap();
            let idx2 = writer.append(&buf).unwrap();
            // read the entry
            let reader = LogReader::new(open(&fpath).unwrap());
            let buf1 = unsafe { reader.at::<Vec<u8>>(idx1.len, idx1.pos).unwrap() };
            let buf2 = unsafe { reader.at::<Vec<u8>>(idx2.len, idx2.pos).unwrap() };
            // succeed if we received the correct data given the positions
            buf == buf1 && buf == buf2
        }
    }

    quickcheck! {
        fn reader_iterates_entries_written_by_writer(entries: Vec<Vec<u8>>) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");
            // write the entries
            let mut writer = LogWriter::new(create(&fpath).unwrap()).unwrap();
            let indices: Vec<LogIndex> = entries.iter().map(|buf| writer.append(&buf).unwrap()).collect();
            // read the entries
            let reader = LogReader::new(open(&fpath).unwrap());
            let mut iter = LogIterator::new(open(&fpath).unwrap()).unwrap();
            for (idx, buf) in indices.iter().zip(entries) {
                let (idx_from_reader, buf_from_reader) = iter.next::<Vec<u8>>().unwrap().unwrap();
                if *idx != idx_from_reader || buf != buf_from_reader {
                    return false;
                }
                let buf_from_reader = unsafe { reader.at::<Vec<u8>>(idx_from_reader.len, idx_from_reader.pos).unwrap() };
                if buf != buf_from_reader {
                    return false;
                }
            }
            // succeed if we could iterate over all written data and received the correct indices
            true
        }
    }
}
