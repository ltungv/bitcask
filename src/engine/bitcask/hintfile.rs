use std::{
    fs,
    io::{self, BufReader, BufWriter, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct HintFileEntry {
    pub tstamp: u128,
    pub len: u64,
    pub pos: u64,
    pub key: Vec<u8>,
}

/// An iterator over all entries in a hint file.
#[derive(Debug)]
pub struct HintFileIterator(BufReader<fs::File>);

impl HintFileIterator {
    /// Create a new iterator over the hint file at the given `path`.
    pub fn open<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        let buf_reader = BufReader::new(file);
        Ok(Self(buf_reader))
    }

    /// Return a next entry in the hint file.
    pub fn entry(&mut self) -> bincode::Result<Option<HintFileEntry>> {
        match bincode::deserialize_from(&mut self.0) {
            Ok(entry) => Ok(Some(entry)),
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

/// An append-only writer for writing entries to the given hint file.
#[derive(Debug)]
pub struct HintFileWriter(BufWriter<fs::File>);

impl HintFileWriter {
    pub fn create<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = fs::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(path)?;
        let writer = BufWriter::new(file);
        Ok(HintFileWriter(writer))
    }

    pub fn append(&mut self, tstamp: u128, len: u64, pos: u64, key: &[u8]) -> bincode::Result<()> {
        let entry = HintFileEntry {
            tstamp,
            len,
            pos,
            key: key.to_vec(),
        };
        bincode::serialize_into(&mut self.0, &entry)
    }

    /// Write all buffered bytes to the underlying I/O device
    pub fn flush(&mut self) -> io::Result<()> {
        self.0.flush()?;
        Ok(())
    }

    /// Synchronize the writer state with the file system and ensure all data is physically written.
    pub fn sync_all(&mut self) -> io::Result<()> {
        self.0.get_ref().sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::quickcheck;

    use super::*;

    quickcheck! {
        fn writer_works(tstamp: u128, len: u64, pos: u64, key: Vec<u8>) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");

            // write the entry
            let mut writer = HintFileWriter::create(fpath).unwrap();
            writer.append(tstamp, len, pos, &key).unwrap();
            writer.flush().unwrap();

            // succeed if there's no error
            true
        }
    }

    quickcheck! {
        fn iterator_iterates_entries_written_by_writer(entries: Vec<(u128, u64, u64, Vec<u8>)>) -> bool {
            let dir = tempfile::tempdir().unwrap();
            let fpath = dir.as_ref().join("test");

            // write the entry
            let mut writer = HintFileWriter::create(&fpath).unwrap();
            for (tstamp, len, pos, key) in entries.iter() {
                writer.append(*tstamp, *len, *pos, key).unwrap();
            }
            writer.flush().unwrap();

            // read the entry
            let mut iter = HintFileIterator::open(&fpath).unwrap();
            let mut valid = true;
            for (tstamp, len, pos, key) in entries.iter() {
                let entry = iter.entry().unwrap();
                match entry {
                    None => return false,
                    Some(e) => {
                        valid &= e.tstamp == *tstamp &&
                            e.len == *len &&
                            e.pos == *pos &&
                            e.key == *key;
                    }
                }
            }

            // succeed if we can iterate over all written data
            valid
        }
    }
}
