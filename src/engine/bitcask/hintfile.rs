use std::{
    fs,
    io::{self, Read, Seek, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HintFileError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("serialization error")]
    Serialization(#[from] bincode::Error),
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct HintFileEntry {
    pub(crate) tstamp: u128,
    pub(crate) len: u64,
    pub(crate) pos: u64,
    pub(crate) key: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct HintFileIterator(fs::File);

impl HintFileIterator {
    pub(crate) fn open<P>(path: P) -> Result<Self, HintFileError>
    where
        P: AsRef<Path>,
    {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        Ok(file.into())
    }
}

impl Read for HintFileIterator {
    fn read(&mut self, b: &mut [u8]) -> std::result::Result<usize, io::Error> {
        self.0.read(b)
    }
}

impl From<fs::File> for HintFileIterator {
    fn from(file: fs::File) -> Self {
        Self(file)
    }
}

impl Iterator for HintFileIterator {
    type Item = Result<HintFileEntry, HintFileError>;

    fn next(&mut self) -> Option<Self::Item> {
        match bincode::deserialize_from(&mut self.0) {
            Err(err) => match err.as_ref() {
                bincode::ErrorKind::Io(io_err) => match io_err.kind() {
                    // TODO: Note down why this is ok
                    io::ErrorKind::UnexpectedEof => None,
                    _ => Some(Err(HintFileError::from(err))),
                },
                _ => Some(Err(HintFileError::from(err))),
            },
            Ok(entry) => Some(Ok(entry)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct HintFileWriter(fs::File);

impl HintFileWriter {
    pub(crate) fn create<P>(path: P) -> Result<Self, HintFileError>
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

    pub(crate) fn append(
        &mut self,
        tstamp: u128,
        len: u64,
        pos: u64,
        key: &[u8],
    ) -> Result<(), HintFileError> {
        let entry = HintFileEntry {
            tstamp,
            len,
            pos,
            key: key.to_vec(),
        };
        bincode::serialize_into(&mut *self, &entry)?;
        Ok(())
    }
}

impl Write for HintFileWriter {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.write(b)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl TryFrom<fs::File> for HintFileWriter {
    type Error = io::Error;

    fn try_from(mut file: fs::File) -> io::Result<Self> {
        file.seek(io::SeekFrom::End(0))?;
        Ok(HintFileWriter(file))
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::quickcheck;

    use super::*;

    quickcheck! {
        fn writer_works(tstamp: u128, len: u64, pos: u64, key: Vec<u8>) -> bool {
            // setup writer
            let datafile = tempfile::Builder::new().append(true).tempfile().unwrap().into_file();
            let mut writer = HintFileWriter::try_from(datafile).unwrap();

            // write the entry
            writer.append(tstamp, len, pos, &key).unwrap();
            writer.flush().unwrap();

            true
        }
    }

    quickcheck! {
        fn iterator_iterates_entries_written_by_writer(entries: Vec<(u128, u64, u64, Vec<u8>)>) -> bool {
            // setup reader and writer
            let datafile = tempfile::Builder::new().append(true).tempfile().unwrap();
            let iter = HintFileIterator::open(datafile.path()).unwrap();
            let mut writer = HintFileWriter::try_from(datafile.into_file()).unwrap();

            // write the entry
            for (tstamp, len, pos, key) in entries.iter() {
                writer.append(*tstamp, *len, *pos, key).unwrap();
            }
            writer.flush().unwrap();

            // read the entry
            let mut valid = true;
            for ((tstamp, len, pos, key), parse_result) in entries.iter().zip(iter) {
                let entry = parse_result.unwrap();
                valid &= entry.tstamp == *tstamp &&
                    entry.len == *len &&
                    entry.pos == *pos &&
                    entry.key == *key;
            }
            valid
        }
    }
}
