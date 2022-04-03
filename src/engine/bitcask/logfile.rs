use std::{
    fs,
    io::{self, Read, Seek, Write},
    path::Path,
};

use serde::{de::DeserializeOwned, Serialize};

use super::bufio::{BufReaderWithPos, BufWriterWithPos};

#[derive(Debug, PartialEq, Eq)]
pub struct LogIndex {
    pub len: u64,
    pub pos: u64,
}

#[derive(Debug)]
pub struct LogWriter(BufWriterWithPos<fs::File>);

impl LogWriter {
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

    /// Serialize the given entry at EOF.
    pub fn append<T>(&mut self, entry: &T) -> bincode::Result<LogIndex>
    where
        T: Serialize,
    {
        let pos = self.0.pos();
        bincode::serialize_into(&mut self.0, entry)?;
        let len = self.0.pos() - pos;
        Ok(LogIndex { len, pos })
    }

    /// Synchronize the writer state with the file system and ensure all data is physically written.
    pub fn sync_all(&mut self) -> io::Result<()> {
        self.0.get_ref().sync_all()?;
        Ok(())
    }
}

impl Write for LogWriter {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.write(b)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

/// An iterator over all entries in a data file and their indices.
///
/// This struct only supports reading in one direction from start to end to iterate over all
/// entries in the given data file. Use [`DataFileReader`] to read entries at random positions.
///
/// [`DataFileReader`]: opal::engine::bitcask::datafile::DataFileReader
#[derive(Debug)]
pub struct LogReader(BufReaderWithPos<fs::File>);

impl LogReader {
    /// Create a new iterator over the data file at the given `path`.
    pub fn open<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        let buf_reader = BufReaderWithPos::new(file)?;
        Ok(Self(buf_reader))
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

    pub fn at<T>(&mut self, pos: u64) -> bincode::Result<(LogIndex, T)>
    where
        T: DeserializeOwned,
    {
        self.seek(io::SeekFrom::Start(pos))?;
        let entry = bincode::deserialize_from(&mut self.0)?;
        let len = self.0.pos() - pos;
        let index = LogIndex { len, pos };
        Ok((index, entry))
    }
}

impl Read for LogReader {
    fn read(&mut self, b: &mut [u8]) -> std::result::Result<usize, std::io::Error> {
        self.0.read(b)
    }
}

impl Seek for LogReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::result::Result<u64, std::io::Error> {
        self.0.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::quickcheck;

    use super::*;

    quickcheck! {
        fn writer_position_updated_after_write(buf: Vec<u8>) -> bool {
            // setup writer
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");

            // write the entry
            let mut writer = LogWriter::create(&fpath).unwrap();
            let idx1 = writer.append(&buf).unwrap();
            let idx2 = writer.append(&buf).unwrap();
            writer.flush().unwrap();

            // succeed if we received the correct index
            idx1.pos == 0 && idx1.len == idx2.pos && idx1.len == idx2.len
        }
    }

    quickcheck! {
        fn reader_reads_entry_written_by_writer(buf: Vec<u8>) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");

            // write the entry
            let mut writer = LogWriter::create(&fpath).unwrap();
            let idx1 = writer.append(&buf).unwrap();
            let idx2 = writer.append(&buf).unwrap();
            writer.flush().unwrap();

            // read the entry
            let mut reader = LogReader::open(&fpath).unwrap();
            let (idx1_from_reader, buf1) = reader.at::<Vec<u8>>(idx1.pos).unwrap();
            let (idx2_from_reader, buf2) = reader.at::<Vec<u8>>(idx2.pos).unwrap();

            // succeed if we received the correct data given the positions
            idx1 == idx1_from_reader && idx2 == idx2_from_reader && buf == buf1 && buf == buf2
        }
    }

    quickcheck! {
        fn reader_iterates_entries_written_by_writer(entries: Vec<Vec<u8>>) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");

            // write the entries
            let mut writer = LogWriter::create(&fpath).unwrap();
            let indices: Vec<LogIndex> = entries.iter().map(|buf| writer.append(&buf).unwrap()).collect();
            writer.flush().unwrap();

            // read the entries
            let mut reader = LogReader::open(&fpath).unwrap();
            for (idx, buf) in indices.iter().zip(entries) {
                let (idx_from_reader, buf_from_reader) = reader.next::<Vec<u8>>().unwrap().unwrap();
                if *idx != idx_from_reader || buf != buf_from_reader {
                    return false;
                }
            }

            // succeed if we could iterate over all written data and received the correct indices
            true
        }
    }
}
