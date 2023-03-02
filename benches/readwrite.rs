#![allow(dead_code)]

use std::num::NonZeroUsize;
use std::time::Duration;

use bytes::Bytes;
use criterion::{black_box, BenchmarkId, SamplingMode};

use ::bitcask::storage::{bitcask, KeyValueStorage};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use pprof::criterion::{Output, PProfProfiler};
use rand::distributions::{Standard, Uniform};
use rand::prelude::*;
use rayon::{
    iter::{IntoParallelIterator, ParallelIterator},
    ThreadPool, ThreadPoolBuilder,
};
use tempfile::TempDir;

const ITER: usize = 10000;
const KEY_SIZE: usize = 1024;
const VAL_SIZE: usize = 8096;

#[derive(Clone)]
struct KeyValuePair(Bytes, Bytes);

impl KeyValuePair {
    fn random<R: Rng>(rng: &mut R, key_size: usize, val_size: usize) -> KeyValuePair {
        let key: Bytes = rng.sample_iter(Standard).take(key_size).collect();
        let val: Bytes = rng.sample_iter(Standard).take(val_size).collect();
        KeyValuePair(key, val)
    }

    fn random_many<R: Rng>(
        rng: &mut R,
        size: usize,
        key_size: usize,
        val_size: usize,
    ) -> Vec<KeyValuePair> {
        let key_dist = Uniform::from(1..key_size);
        let val_dist = Uniform::from(1..val_size);
        (0..size)
            .into_iter()
            .map(|_| {
                let ksz = key_dist.sample(rng);
                let vsz = val_dist.sample(rng);
                KeyValuePair::random(rng, ksz, vsz)
            })
            .collect()
    }
}

fn get_bitcask() -> (bitcask::Bitcask, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    let bitcask = bitcask::Config::default()
        .concurrency(NonZeroUsize::new(num_cpus::get_physical()).unwrap())
        .path(tmpdir.path())
        .to_owned()
        .open()
        .unwrap();
    (bitcask, tmpdir)
}

fn get_threadpool(nthreads: usize) -> ThreadPool {
    ThreadPoolBuilder::new()
        .num_threads(nthreads)
        .build()
        .unwrap()
}

fn concurrent_write<E>(engine: E, kv_pairs: Vec<KeyValuePair>)
where
    E: KeyValueStorage,
{
    kv_pairs
        .into_par_iter()
        .for_each_with(engine, |engine, kv| {
            engine.set(black_box(kv.0), black_box(kv.1)).unwrap();
        });
}

fn concurrent_read<E>(engine: E, kv_pairs: Vec<KeyValuePair>)
where
    E: KeyValueStorage,
{
    kv_pairs
        .into_par_iter()
        .for_each_with(engine, |engine, kv| {
            engine.get(black_box(kv.0)).unwrap();
        });
}

fn sequential_write<E>(engine: E, kv_pairs: Vec<KeyValuePair>)
where
    E: KeyValueStorage,
{
    kv_pairs.into_iter().for_each(|kv| {
        engine.set(black_box(kv.0), black_box(kv.1)).unwrap();
    });
}

fn sequential_read<E>(engine: E, kv_pairs: Vec<KeyValuePair>)
where
    E: KeyValueStorage,
{
    kv_pairs.into_iter().for_each(|kv| {
        engine.get(black_box(kv.0)).unwrap();
    });
}

fn bench_write(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64((1 << 7) + 1);
    let kv_pairs = KeyValuePair::random_many(&mut rng, ITER, KEY_SIZE, VAL_SIZE);
    let nbytes = kv_pairs
        .iter()
        .fold(0, |acc, kv| acc + kv.0.len() + kv.1.len());

    let mut g = c.benchmark_group("bitcask_write");
    g.sampling_mode(SamplingMode::Flat);
    g.throughput(Throughput::Bytes(nbytes as u64));

    g.bench_function("sequential", |b| {
        let (engine, _tmpdir) = get_bitcask();
        b.iter_batched(
            || {
                let mut kv_pairs = kv_pairs.clone();
                kv_pairs.shuffle(&mut rng);
                (engine.get_handle(), kv_pairs)
            },
            |(engine, kvs)| black_box(sequential_write(engine, kvs)),
            BatchSize::LargeInput,
        );
    });
    for i in 1..=3 {
        let concurrency = 1 << i as usize;
        g.bench_with_input(
            BenchmarkId::new("concurrent", concurrency),
            &concurrency,
            |b, concurrency| {
                get_threadpool(*concurrency as usize).install(|| {
                    let (engine, _tmpdir) = get_bitcask();
                    b.iter_batched(
                        || {
                            let mut kv_pairs = kv_pairs.clone();
                            kv_pairs.shuffle(&mut rng);
                            (engine.get_handle(), kv_pairs)
                        },
                        |(engine, kvs)| black_box(concurrent_write(engine, kvs)),
                        BatchSize::LargeInput,
                    );
                });
            },
        );
    }
    g.finish();
}

fn bench_read(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64((1 << 7) + 1);
    let kv_pairs = KeyValuePair::random_many(&mut rng, ITER, KEY_SIZE, VAL_SIZE);
    let nbytes = kv_pairs
        .iter()
        .fold(0, |acc, kv| acc + kv.0.len() + kv.1.len());

    let mut g = c.benchmark_group("bitcask_read");
    g.sampling_mode(SamplingMode::Flat);
    g.throughput(Throughput::Bytes(nbytes as u64));

    g.bench_function("sequential", |b| {
        let (engine, _tmpdir) = get_bitcask();
        let handle = engine.get_handle();
        kv_pairs.iter().cloned().for_each(|kv| {
            handle.set(kv.0, kv.1).unwrap();
        });
        b.iter_batched(
            || {
                let mut kv_pairs = kv_pairs.clone();
                kv_pairs.shuffle(&mut rng);
                (engine.get_handle(), kv_pairs)
            },
            |(engine, kvs)| black_box(sequential_read(engine, kvs)),
            BatchSize::LargeInput,
        );
    });
    for concurrency in [2, 4, 6, 8] {
        g.bench_with_input(
            BenchmarkId::new("concurrent", concurrency),
            &concurrency,
            |b, concurrency| {
                let pool = get_threadpool(*concurrency as usize);
                pool.install(|| {
                    let (engine, _tmpdir) = get_bitcask();
                    let handle = engine.get_handle();
                    kv_pairs.iter().cloned().for_each(|kv| {
                        handle.set(kv.0, kv.1).unwrap();
                    });
                    b.iter_batched(
                        || {
                            let mut kv_pairs = kv_pairs.clone();
                            kv_pairs.shuffle(&mut rng);
                            (engine.get_handle(), kv_pairs)
                        },
                        |(engine, kvs)| black_box(concurrent_read(engine, kvs)),
                        BatchSize::LargeInput,
                    );
                });
            },
        );
    }
    g.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(500, Output::Flamegraph(None)))
        .sample_size(100)
        .confidence_level(0.99)
        .significance_level(0.01)
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(10));
    targets = bench_write, bench_read,
);
criterion_main!(benches);
