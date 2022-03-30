use bytes::Bytes;
use opal::engine::{InMemoryStorage, LogStructuredHashTable, SledKeyValueStore};
use rand::{distributions::Alphanumeric, prelude::*};
use tempfile::TempDir;

pub fn prep_lfs() -> (LogStructuredHashTable, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    let phys_cpus = num_cpus::get_physical();
    let engine = LogStructuredHashTable::open(tmpdir.path(), phys_cpus).unwrap();
    (engine, tmpdir)
}

pub fn prep_sled() -> (SledKeyValueStore, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    let db = sled::Config::default().path(tmpdir.path()).open().unwrap();
    let engine = SledKeyValueStore::new(db);
    (engine, tmpdir)
}

pub fn prep_inmem() -> (InMemoryStorage, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    let engine = InMemoryStorage::default();
    (engine, tmpdir)
}

pub fn prebuilt_kv_pairs<R>(
    rng: &mut R,
    size: usize,
    key_size: usize,
    val_size: usize,
) -> Vec<(String, Bytes)>
where
    R: Rng,
{
    (0..size)
        .into_iter()
        .map(|_| rand_key_value(rng, key_size, val_size))
        .collect()
}

pub fn rand_key_value<R>(rng: &mut R, key_size: usize, val_size: usize) -> (String, Bytes)
where
    R: Rng,
{
    let key: String = rng
        .sample_iter(Alphanumeric)
        .take(key_size)
        .map(char::from)
        .collect();
    let val: String = rng
        .sample_iter(Alphanumeric)
        .take(val_size)
        .map(char::from)
        .collect();
    (key, val.into())
}
