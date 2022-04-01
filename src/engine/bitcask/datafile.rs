use std::{
    fs,
    io::{self, Read, Seek, Write},
    path::Path,
};

use bytes::Buf;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataFileError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("serialization error")]
    Serialization(#[from] bincode::Error),
}

#[derive(Debug)]
pub(crate) struct DataFileIndex {
    pub(crate) len: u64,
    pub(crate) pos: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub(crate) enum DataFileEntryValue {
    Tombstone,
    Data(Vec<u8>),
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct DataFileEntry {
    pub(crate) tstamp: u128,
    pub(crate) key: Vec<u8>,
    pub(crate) value: DataFileEntryValue,
}

#[derive(Debug)]
pub(crate) struct DataFileIterator {
    pos: u64,
    file: fs::File,
}

impl DataFileIterator {
    pub(crate) fn open<P>(path: P) -> Result<Self, DataFileError>
    where
        P: AsRef<Path>,
    {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        let iter = file.try_into()?;
        Ok(iter)
    }
}

impl TryFrom<fs::File> for DataFileIterator {
    type Error = io::Error;

    fn try_from(mut file: fs::File) -> io::Result<Self> {
        let pos = file.seek(io::SeekFrom::Start(0))?;
        Ok(DataFileIterator { pos, file })
    }
}

impl Read for DataFileIterator {
    fn read(&mut self, b: &mut [u8]) -> std::result::Result<usize, io::Error> {
        self.file.read(b).map(|bytes_read| {
            self.pos += bytes_read as u64;
            bytes_read
        })
    }
}

impl Iterator for DataFileIterator {
    type Item = Result<(DataFileIndex, DataFileEntry), DataFileError>;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.pos;
        match bincode::deserialize_from(&mut *self) {
            Err(err) => match err.as_ref() {
                bincode::ErrorKind::Io(io_err) => match io_err.kind() {
                    // TODO: Note down why this is ok
                    io::ErrorKind::UnexpectedEof => None,
                    _ => Some(Err(DataFileError::from(err))),
                },
                _ => Some(Err(DataFileError::from(err))),
            },
            Ok(entry) => {
                let len = self.pos - pos;
                let index = DataFileIndex { len, pos };
                Some(Ok((index, entry)))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct DataFileReader(fs::File);

impl DataFileReader {
    pub(crate) fn open<P>(path: P) -> Result<Self, DataFileError>
    where
        P: AsRef<Path>,
    {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        Ok(file.into())
    }

    /// # Unsafe
    ///
    /// By calling this method, we guaranteed that the data at `pos` with length `len` is valid and
    /// can be parse into a `DataFileEntry`
    pub(crate) unsafe fn entry_at(
        &self,
        index: &DataFileIndex,
    ) -> Result<DataFileEntry, DataFileError> {
        let mmap = memmap2::MmapOptions::new()
            .offset(index.pos)
            .len(index.len as usize)
            .map_copy_read_only(&self.0)?;
        let entry = bincode::deserialize(&mmap)?;
        Ok(entry)
    }

    /// # Unsafe
    ///
    /// By calling this method, we guaranteed that the data at `pos` with length `len` is valid and
    /// can be parse into a `DataFileEntry`
    pub(crate) unsafe fn copy_raw<W>(
        &self,
        index: &DataFileIndex,
        dst: &mut W,
    ) -> Result<(), DataFileError>
    where
        W: Write,
    {
        let mmap = memmap2::MmapOptions::new()
            .offset(index.pos)
            .len(index.len as usize)
            .map_copy_read_only(&self.0)?;
        io::copy(&mut mmap.reader(), dst)?;
        Ok(())
    }
}

impl From<fs::File> for DataFileReader {
    fn from(file: fs::File) -> Self {
        DataFileReader(file)
    }
}

#[derive(Debug)]
pub(crate) struct DataFileWriter {
    pos: u64,
    file: fs::File,
}

impl DataFileWriter {
    pub(crate) fn create<P>(path: P) -> Result<Self, DataFileError>
    where
        P: AsRef<Path>,
    {
        let file = fs::OpenOptions::new()
            .read(false)
            .append(true)
            .create_new(true)
            .open(path)?;
        let writer = file.try_into()?;
        Ok(writer)
    }

    pub(crate) fn append(&mut self, entry: &DataFileEntry) -> Result<DataFileIndex, DataFileError> {
        let pos = self.pos;
        bincode::serialize_into(&mut *self, entry)?;
        let len = self.pos - pos;
        Ok(DataFileIndex { pos, len })
    }

    pub(crate) fn pos(&self) -> u64 {
        self.pos
    }

    pub(crate) fn sync(&self) -> Result<(), DataFileError> {
        self.file.sync_all()?;
        Ok(())
    }
}

impl Write for DataFileWriter {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.file.write(b).map(|bytes_written| {
            self.pos += bytes_written as u64;
            bytes_written
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl TryFrom<fs::File> for DataFileWriter {
    type Error = io::Error;

    fn try_from(mut file: fs::File) -> io::Result<Self> {
        let pos = file.seek(io::SeekFrom::End(0))?;
        Ok(DataFileWriter { pos, file })
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::quickcheck;

    use super::*;

    quickcheck! {
        fn writer_position_initialized_and_updated_after_write(tstamp: u128, key: Vec<u8>, value: Vec<u8>) -> bool {
            // setup writer
            let datafile = tempfile::Builder::new().append(true).tempfile().unwrap().into_file();
            let mut writer = DataFileWriter::try_from(datafile).unwrap();

            // get the serialized length
            let entry = DataFileEntry { tstamp, key, value: DataFileEntryValue::Data(value) };
            let bytes = bincode::serialize(&entry).unwrap().len() as u64;

            // record current position and write the entry
            let pos_init = writer.pos;
            let idx = writer.append(&entry).unwrap();
            writer.flush().unwrap();

            // check if returned index matched previous position and serialized length
            // check if writer's position is updated
            idx.pos == pos_init && idx.len == bytes && writer.pos == bytes
        }
    }

    quickcheck! {
        fn reader_reads_entry_written_by_writer(tstamp: u128, key: Vec<u8>, value: Vec<u8>) -> bool {
            // setup reader and writer
            let datafile = tempfile::Builder::new().append(true).tempfile().unwrap();
            let reader = DataFileReader::open(datafile.path()).unwrap();
            let mut writer = DataFileWriter::try_from(datafile.into_file()).unwrap();

            // write the entry
            let entry = DataFileEntry { tstamp, key, value: DataFileEntryValue::Data(value) };
            let idx = writer.append(&entry).unwrap();
            writer.flush().unwrap();

            // read the entry
            let entry_from_disk = unsafe { reader.entry_at(&idx).unwrap() };

            // check if the read entry is the written entry
            entry.tstamp == entry_from_disk.tstamp &&
                entry.key == entry_from_disk.key &&
                entry.value == entry_from_disk.value
        }
    }

    quickcheck! {
        fn iterator_iterates_entries_written_by_writer(entries: Vec<(u128, Vec<u8>, Vec<u8>)>) -> bool {
            // setup reader and writer
            let datafile = tempfile::Builder::new().append(true).tempfile().unwrap();
            let iter = DataFileIterator::open(datafile.path()).unwrap();
            let reader = DataFileReader::open(datafile.path()).unwrap();
            let mut writer = DataFileWriter::try_from(datafile.into_file()).unwrap();

            // write the entry
            for (tstamp, key, value) in entries.iter() {
                let entry = DataFileEntry { tstamp: *tstamp, key: key.clone(), value: DataFileEntryValue::Data(value.clone()) };
                writer.append(&entry).unwrap();
            }
            writer.flush().unwrap();

            // read the entry
            let mut valid = true;
            for ((tstamp, key, value), parse_result) in entries.iter().zip(iter) {
                let (index, entry) = parse_result.unwrap();
                let entry_from_reader = unsafe { reader.entry_at(&index).unwrap() };
                valid &= entry.tstamp == *tstamp &&
                    entry.key == *key &&
                    entry.value == DataFileEntryValue::Data(value.clone());
                valid &= entry.tstamp == entry_from_reader.tstamp &&
                    entry.key == entry_from_reader.key &&
                    entry.value == entry_from_reader.value;
            }
            valid
        }
    }
}
