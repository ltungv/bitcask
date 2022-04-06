use bytes::Bytes;
use criterion::black_box;
use opal::storage::{
    BitcaskKeyValueStorage, Config, DashMapKeyValueStorage, KeyValueStorage, SledKeyValueStorage,
};
use rand::{
    distributions::{Standard, Uniform},
    prelude::*,
};
use rayon::{
    iter::{IntoParallelIterator, ParallelIterator},
    ThreadPool, ThreadPoolBuilder,
};
use tempfile::TempDir;

pub type KeyValuePair = (Bytes, Bytes);

#[derive(Debug)]
pub enum EngineType {
    Bitcask,
    Sled,
    DashMap,
}

pub fn concurrent_write_bulk_bench_iter<E>(
    (engine, kv_pairs, _tmpdir): (E, Vec<KeyValuePair>, TempDir),
) where
    E: KeyValueStorage,
{
    concurrent_write_bulk_bench_iter_no_tempdir((engine, kv_pairs));
}

pub fn concurrent_write_bulk_bench_iter_no_tempdir<E>((engine, kv_pairs): (E, Vec<KeyValuePair>))
where
    E: KeyValueStorage,
{
    kv_pairs
        .into_par_iter()
        .for_each_with(engine, |engine, (k, v)| {
            engine.set(black_box(k), black_box(v)).unwrap();
        });
}

pub fn concurrent_read_bulk_bench_iter<E>((engine, kv_pairs): (E, Vec<KeyValuePair>))
where
    E: KeyValueStorage,
{
    kv_pairs
        .into_par_iter()
        .for_each_with(engine, |engine, (k, _)| {
            engine.get(black_box(&k)).unwrap();
        });
}

pub fn sequential_write_bulk_bench_iter<E>(
    (engine, kv_pairs, _tmpdir): (E, Vec<KeyValuePair>, TempDir),
) where
    E: KeyValueStorage,
{
    sequential_write_bulk_bench_iter_no_tempdir((engine, kv_pairs))
}

pub fn sequential_write_bulk_bench_iter_no_tempdir<E>((engine, kv_pairs): (E, Vec<KeyValuePair>))
where
    E: KeyValueStorage,
{
    kv_pairs.into_iter().for_each(|(k, v)| {
        engine.set(black_box(k), black_box(v)).unwrap();
    });
}

pub fn sequential_read_bulk_bench_iter<E>((engine, kv_pairs): (E, Vec<KeyValuePair>))
where
    E: KeyValueStorage,
{
    kv_pairs.into_iter().for_each(|(k, _)| {
        engine.get(black_box(&k)).unwrap();
    });
}

pub fn get_bitcask() -> (BitcaskKeyValueStorage, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    let bitcask = Config::default().open(tmpdir.path()).unwrap();
    let engine = BitcaskKeyValueStorage::from(bitcask);
    (engine, tmpdir)
}

pub fn get_sled() -> (SledKeyValueStorage, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    let db = sled::Config::default().path(tmpdir.path()).open().unwrap();
    let engine = SledKeyValueStorage::new(db);
    (engine, tmpdir)
}

pub fn get_dashmap() -> (DashMapKeyValueStorage, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    let engine = DashMapKeyValueStorage::default();
    (engine, tmpdir)
}

pub fn get_threadpool(nthreads: usize) -> ThreadPool {
    ThreadPoolBuilder::new()
        .num_threads(nthreads)
        .build()
        .unwrap()
}

pub fn rand_kv_pairs(size: usize, key_size: usize, val_size: usize) -> Vec<KeyValuePair> {
    let mut rng = StdRng::from_seed([0u8; 32]);
    let key_dist = Uniform::from(1..key_size);
    let val_dist = Uniform::from(1..val_size);
    (0..size)
        .into_iter()
        .map(|_| {
            let ksz = key_dist.sample(&mut rng);
            let vsz = val_dist.sample(&mut rng);
            rand_kv_pair(&mut rng, ksz, vsz)
        })
        .collect()
}

pub fn rand_kv_pair<R>(rng: &mut R, key_size: usize, val_size: usize) -> KeyValuePair
where
    R: Rng,
{
    let key: Bytes = rng.sample_iter(Standard).take(key_size).collect();
    let val: Bytes = rng.sample_iter(Standard).take(val_size).collect();
    (key, val)
}
