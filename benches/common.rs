use opal::engine::{BitCaskConfig, BitCaskKeyValueStore, DashMapKeyValueStore, SledKeyValueStore};
use rand::{
    distributions::{Standard, Uniform},
    prelude::*,
};
use rayon::{ThreadPool, ThreadPoolBuilder};
use tempfile::TempDir;

pub fn get_bitcask() -> (BitCaskKeyValueStore, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    let engine = BitCaskKeyValueStore(BitCaskConfig::default().open(tmpdir.path()).unwrap());
    (engine, tmpdir)
}

pub fn get_sled() -> (SledKeyValueStore, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    let db = sled::Config::default().path(tmpdir.path()).open().unwrap();
    let engine = SledKeyValueStore::new(db);
    (engine, tmpdir)
}

pub fn get_dashmap() -> (DashMapKeyValueStore, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    let engine = DashMapKeyValueStore::default();
    (engine, tmpdir)
}

pub fn get_threadpool(nthreads: usize) -> ThreadPool {
    ThreadPoolBuilder::new()
        .num_threads(nthreads)
        .build()
        .unwrap()
}

pub fn prebuilt_kv_pairs(size: usize, key_size: usize, val_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::from_seed([0u8; 32]);
    let key_dist = Uniform::from(1..key_size);
    let val_dist = Uniform::from(1..val_size);
    (0..size)
        .into_iter()
        .map(|_| {
            let ksz = key_dist.sample(&mut rng);
            let vsz = val_dist.sample(&mut rng);
            rand_key_value(&mut rng, ksz, vsz)
        })
        .collect()
}

pub fn rand_key_value<R>(rng: &mut R, key_size: usize, val_size: usize) -> (Vec<u8>, Vec<u8>)
where
    R: Rng,
{
    let key: Vec<u8> = rng.sample_iter(Standard).take(key_size).collect();
    let val: Vec<u8> = rng.sample_iter(Standard).take(val_size).collect();
    (key, val)
}
