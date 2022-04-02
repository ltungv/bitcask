use std::{
    fs,
    io::{self, Write},
    path::Path,
};

use bytes::Buf;
use serde::{Deserialize, Serialize};

use super::bufio::{BufReaderWithPos, BufWriterWithPos};

/// The position of a data entry within the data file.
#[derive(Debug)]
pub struct DataFileIndex {
    /// The serialized byte size of the data entry.
    pub len: u64,
    /// The position of the data entry given as an offset from the beginning of the entry.
    pub pos: u64,
}

/// The data entry that gets appended to the data file on write.
#[derive(Serialize, Deserialize, Debug)]
pub struct DataFileEntry {
    /// Local unix nano timestamp.
    pub tstamp: u128,
    /// Data of the key.
    pub key: Vec<u8>,
    /// Data of the value.
    pub value: DataFileEntryValue,
}

/// The data file entry's value.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum DataFileEntryValue {
    /// The key has been deleted.
    Tombstone,
    /// The value of the key.
    Data(Vec<u8>),
}

/// An iterator over all entries in a data file and their indices.
///
/// This struct only supports reading in one direction from start to end to iterate over all
/// entries in the given data file. Use [`DataFileReader`] to read entries at random positions.
///
/// [`DataFileReader`]: opal::engine::bitcask::datafile::DataFileReader
#[derive(Debug)]
pub struct DataFileIterator(BufReaderWithPos<fs::File>);

impl DataFileIterator {
    /// Create a new iterator over the data file at the given `path`.
    pub fn open<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        let buf_reader = BufReaderWithPos::new(file)?;
        Ok(Self(buf_reader))
    }

    /// Return a next entry in the data file.
    pub fn entry(&mut self) -> bincode::Result<Option<(DataFileIndex, DataFileEntry)>> {
        // get reader current position so we can calculate the number of serialized bytes
        let pos = self.0.pos();

        match bincode::deserialize_from(&mut self.0) {
            Ok(entry) => {
                let len = self.0.pos() - pos;
                let index = DataFileIndex { len, pos };
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

/// A random-access reader for entries in an data file given their indices.
///
/// This struct only supports reading entries when knowing theirs indices. To go over all entries
/// without having to know the indices, use [`DataFileIterator`].
///
/// [`DataFileIterator`]: opal::engine::bitcask::datafile::DataFileIterator
#[derive(Debug)]
pub struct DataFileReader(fs::File);

impl DataFileReader {
    /// Create a new reader for data entries in the given `path`.
    pub fn open<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        Ok(Self(file))
    }

    /// Return the entry at the given index in the data file.
    ///
    /// # Unsafe
    ///
    /// When using this method, we must maintain the invariant that reads will always access a
    /// valid region. For BitCask, we ensure this by only appending to data files and keeping
    /// the regions where we can index into immutable.
    pub unsafe fn entry(&self, len: u64, pos: u64) -> bincode::Result<DataFileEntry> {
        let mmap = memmap2::MmapOptions::new()
            .offset(pos)
            .len(len as usize)
            .map(&self.0)?;
        bincode::deserialize(&mmap)
    }

    /// Copy the entry at the given file location to another file.
    ///
    /// # Unsafe
    ///
    /// When using this method, we must maintain the invariant that reads will always access a
    /// valid region. For BitCask, we ensure this by only appending to data files and keeping
    /// the regions where we can index into immutable.
    pub unsafe fn copy<W>(&self, len: u64, pos: u64, dst: &mut W) -> io::Result<u64>
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

/// An append-only writer for writing entries to the given data file.
#[derive(Debug)]
pub struct DataFileWriter(BufWriterWithPos<fs::File>);

impl DataFileWriter {
    /// Create a new data file for writing entries to.
    pub fn create<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = fs::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(path)?;
        let writer = BufWriterWithPos::new(file)?;
        Ok(Self(writer))
    }

    /// Append an entry that contains data.
    pub fn data(
        &mut self,
        tstamp: u128,
        key: &[u8],
        value: &[u8],
    ) -> bincode::Result<DataFileIndex> {
        let entry = DataFileEntry {
            tstamp,
            key: key.to_vec(),
            value: DataFileEntryValue::Data(value.to_vec()),
        };
        self.append(&entry)
    }

    /// Append an entry that contains the tombstone.
    pub fn tombstone(&mut self, tstamp: u128, key: &[u8]) -> bincode::Result<DataFileIndex> {
        let entry = DataFileEntry {
            tstamp,
            key: key.to_vec(),
            value: DataFileEntryValue::Tombstone,
        };
        self.append(&entry)
    }

    /// Return the last written position.
    pub fn pos(&self) -> u64 {
        self.0.pos()
    }

    /// Return a underlying writer.
    pub fn writer(&mut self) -> &mut BufWriterWithPos<fs::File> {
        &mut self.0
    }

    /// Write all buffered bytes to the underlying I/O device
    pub fn flush(&mut self) -> io::Result<()> {
        self.0.flush()?;
        Ok(())
    }

    /// Synchronize the writer state with the file system and ensure all data is physically written.
    pub fn sync_all(&mut self) -> io::Result<()> {
        self.0.sync_all()?;
        Ok(())
    }

    fn append(&mut self, entry: &DataFileEntry) -> bincode::Result<DataFileIndex> {
        let pos = self.0.pos();
        bincode::serialize_into(&mut self.0, entry)?;
        let len = self.0.pos() - pos;
        Ok(DataFileIndex { pos, len })
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::quickcheck;

    use super::*;

    quickcheck! {
        fn writer_position_initialized_and_updated_after_write(tstamp: u128, key: Vec<u8>, value: Vec<u8>) -> bool {
            // setup writer
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");
            let mut writer = DataFileWriter::create(fpath).unwrap();

            // get the serialized length
            let entry = DataFileEntry { tstamp, key, value: DataFileEntryValue::Data(value) };
            let nbytes = bincode::serialize(&entry).unwrap().len() as u64;

            // record current position and write the entry
            let pos_init = writer.pos();
            let idx = writer.append(&entry).unwrap();
            writer.flush().unwrap();

            // check if returned index matched previous position and serialized length
            // check if writer's position is updated
            idx.pos == pos_init && idx.len == nbytes && writer.pos() == nbytes
        }
    }

    quickcheck! {
        fn reader_reads_entry_written_by_writer(tstamp: u128, key: Vec<u8>, value: Vec<u8>) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");

            // write the entry
            let mut writer = DataFileWriter::create(&fpath).unwrap();
            let entry = DataFileEntry { tstamp, key, value: DataFileEntryValue::Data(value) };
            let idx = writer.append(&entry).unwrap();
            writer.flush().unwrap();

            // read the entry
            let reader = DataFileReader::open(&fpath).unwrap();
            let entry_from_disk = unsafe { reader.entry(idx.len, idx.pos).unwrap() };

            // check if the read entry is the written entry
            entry.tstamp == entry_from_disk.tstamp &&
                entry.key == entry_from_disk.key &&
                entry.value == entry_from_disk.value
        }
    }

    quickcheck! {
        fn iterator_iterates_entries_written_by_writer(entries: Vec<(u128, Vec<u8>, Vec<u8>)>) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");

            // write the entry
            let mut writer = DataFileWriter::create(&fpath).unwrap();
            for (tstamp, key, value) in entries.iter() {
                let entry = DataFileEntry { tstamp: *tstamp, key: key.clone(), value: DataFileEntryValue::Data(value.clone()) };
                writer.append(&entry).unwrap();
            }
            writer.flush().unwrap();

            // read the entry
            let reader = DataFileReader::open(&fpath).unwrap();
            let mut iter = DataFileIterator::open(&fpath).unwrap();
            let mut valid = true;
            for (tstamp, key, value) in entries.iter() {
                let (index, entry) = iter.entry().unwrap().unwrap();
                let entry_from_reader = unsafe { reader.entry(index.len, index.pos).unwrap() };

                valid &= entry.tstamp == *tstamp &&
                    entry.key == *key &&
                    entry.value == DataFileEntryValue::Data(value.clone());

                valid &= entry.tstamp == entry_from_reader.tstamp &&
                    entry.key == entry_from_reader.key &&
                    entry.value == entry_from_reader.value;
            }

            // succeed if we can iterate over all written data and receive the correct indices
            valid
        }
    }
}
