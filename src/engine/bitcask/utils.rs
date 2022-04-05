use std::{
    collections::BTreeSet,
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
    time,
};

const DATAFILE_EXT: &str = "data";

const HINTFILE_EXT: &str = "hint";

/// Return the data file name given its ID.
pub fn datafile_name<P>(path: P, fileid: u64) -> PathBuf
where
    P: AsRef<Path>,
{
    path.as_ref()
        .join(format!("{}.bitcask.{}", fileid, DATAFILE_EXT))
}

/// Return the hint file name given its ID.
pub fn hintfile_name<P>(path: P, fileid: u64) -> PathBuf
where
    P: AsRef<Path>,
{
    path.as_ref()
        .join(format!("{}.bitcask.{}", fileid, HINTFILE_EXT))
}

/// Returns a list of sorted file IDs by parsing the data file names in the directory.
pub fn sorted_fileids<P>(path: P) -> io::Result<impl Iterator<Item = u64>>
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
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn fileids_sorted_correctly(n in 0..100u64) {
            // keep the file handles until end of scope
            let mut tmps = Vec::new();
            // Create random datafiles and hintfiles in the directory
            let dir = tempfile::tempdir().unwrap();
            for fileid in 0..n {
                tmps.push(fs::File::create(datafile_name(&dir, fileid)).unwrap());
                if rand::random() {
                    tmps.push(fs::File::create(hintfile_name(&dir, fileid)).unwrap());
                }
            }
            // check if ids are sorted
            let fileids = sorted_fileids(dir).unwrap();
            prop_assert!(fileids.enumerate().all(|(i, v)| i as u64 == v))
        }
    }
}
