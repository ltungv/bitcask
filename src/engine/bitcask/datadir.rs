use std::{collections::BTreeMap, io, path::Path};

use super::{datafile::DataFileReader, utils};

#[derive(Debug, Default)]
pub struct DataDir(BTreeMap<u64, DataFileReader>);

impl DataDir {
    pub fn get<P>(&mut self, path: P, fileid: u64) -> io::Result<&DataFileReader>
    where
        P: AsRef<Path>,
    {
        let reader = self.0.entry(fileid).or_insert(DataFileReader::open(
            path.as_ref().join(utils::datafile_name(fileid)),
        )?);
        Ok(reader)
    }

    pub fn stale_fileids(&self, min_fileid: u64) -> Vec<u64> {
        self.0
            .keys()
            .filter(|&&id| id < min_fileid)
            .cloned()
            .collect()
    }

    pub fn drop_stale(&mut self, min_fileid: u64) {
        self.stale_fileids(min_fileid).iter().for_each(|id| {
            self.0.remove(id);
        });
    }
}
