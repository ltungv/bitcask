use std::{collections::BTreeSet, ffi::OsStr, fs, io, path::Path, time};

const DATAFILE_EXT: &str = "data";

const HINTFILE_EXT: &str = "hint";

pub fn datafile_name(fileid: u64) -> String {
    format!("{}.bitcask.{}", fileid, DATAFILE_EXT)
}

pub fn hintfile_name(fileid: u64) -> String {
    format!("{}.bitcask.{}", fileid, HINTFILE_EXT)
}

pub fn sorted_fileids<P>(path: P) -> io::Result<Vec<u64>>
where
    P: AsRef<Path>,
{
    // read directory
    let fileids: BTreeSet<u64> = fs::read_dir(&path)?
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
        .collect();

    let fileids: Vec<u64> = fileids.into_iter().collect();
    Ok(fileids)
}

pub fn timestamp() -> u128 {
    time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .expect("invalid system time")
        .as_nanos()
}
