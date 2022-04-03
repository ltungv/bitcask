use std::{collections::BTreeMap, io, path::Path};

use super::{logfile::LogReader, utils};

#[derive(Debug, Default)]
pub struct LogDir(BTreeMap<u64, LogReader>);

impl LogDir {
    pub fn get<P>(&mut self, path: P, fileid: u64) -> io::Result<&mut LogReader>
    where
        P: AsRef<Path>,
    {
        let reader = self.0.entry(fileid).or_insert(LogReader::open(
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
