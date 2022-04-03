use std::{
    collections::{btree_set::IntoIter, BTreeSet},
    ffi::OsStr,
    fs, io,
    path::Path,
    time,
};

const DATAFILE_EXT: &str = "data";

const HINTFILE_EXT: &str = "hint";

/// Return the data file name given its ID.
pub fn datafile_name(fileid: u64) -> String {
    format!("{}.bitcask.{}", fileid, DATAFILE_EXT)
}

/// Return the hint file name given its ID.
pub fn hintfile_name(fileid: u64) -> String {
    format!("{}.bitcask.{}", fileid, HINTFILE_EXT)
}

/// Returns a list of sorted file IDs by parsing the data file names in the directory.
pub fn sorted_fileids<P>(path: P) -> io::Result<IntoIter<u64>>
where
    P: AsRef<Path>,
{
    // read directory
    Ok(fs::read_dir(&path)?
        // ignore errors
        .filter_map(std::result::Result::ok)
        // extract paths
        .map(|e| e.path())
        // get files with data file extensions
        .filter(|p| p.is_file() && p.extension() == Some(OsStr::new(DATAFILE_EXT)))
        // parse the file id as u64
        .filter_map(|p| {
            p.file_stem()
                .and_then(OsStr::to_str)
                .and_then(|s| s.split('.').next())
                .map(str::parse::<u64>)
        })
        .filter_map(std::result::Result::ok)
        .collect::<BTreeSet<u64>>()
        .into_iter())
}

/// Return system unix nano timestamp
pub fn timestamp() -> u128 {
    time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .expect("invalid system time")
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::quickcheck;

    quickcheck! {
        fn fileids_sorted_correctly(n: u64) -> bool {
            // keep the file handles until end of scope
            let mut tmps = Vec::new();
            // Create random datafiles and hintfiles in the directory
            let dir = tempfile::tempdir().unwrap();
            for fileid in 0..n.min(100) {
                tmps.push(fs::File::create(dir.path().join(datafile_name(fileid))).unwrap());
                if rand::random() {
                    tmps.push(fs::File::create(dir.path().join(hintfile_name(fileid))).unwrap());
                }
            }

            // check if ids are sorted
            let fileids = sorted_fileids(dir).unwrap();
            fileids.enumerate().all(|(i, v)| i as u64 == v)
        }
    }
}
