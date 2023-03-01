#![allow(dead_code)]

use bytes::Bytes;
use criterion::black_box;

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

const ITER: usize = 1000;
const KEY_SIZE: usize = 64;
const VAL_SIZE: usize = 256;

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

fn concurrent_write<E>((engine, kv_pairs): (E, Vec<KeyValuePair>))
where
    E: KeyValueStorage,
{
    black_box(
        kv_pairs
            .into_par_iter()
            .for_each_with(engine, |engine, kv| {
                engine.set(black_box(kv.0), black_box(kv.1)).unwrap();
            }),
    );
}

fn concurrent_read<E>((engine, kv_pairs): (E, Vec<KeyValuePair>))
where
    E: KeyValueStorage,
{
    black_box(
        kv_pairs
            .into_par_iter()
            .for_each_with(engine, |engine, kv| {
                engine.get(black_box(kv.0)).unwrap();
            }),
    );
}

fn sequential_write<E>((engine, kv_pairs): (E, Vec<KeyValuePair>))
where
    E: KeyValueStorage,
{
    black_box(kv_pairs.into_iter().for_each(|kv| {
        engine.set(black_box(kv.0), black_box(kv.1)).unwrap();
    }));
}

fn sequential_read<E>((engine, kv_pairs): (E, Vec<KeyValuePair>))
where
    E: KeyValueStorage,
{
    black_box(kv_pairs.into_iter().for_each(|kv| {
        engine.get(black_box(kv.0)).unwrap();
    }));
}

fn bench_write(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64((1 << 7) + 1);
    let kv_pairs = KeyValuePair::random_many(&mut rng, ITER, KEY_SIZE, VAL_SIZE);
    let nbytes = kv_pairs
        .iter()
        .fold(0, |acc, kv| acc + kv.0.len() + kv.1.len());

    let mut g = c.benchmark_group("bitcask_write");
    g.throughput(Throughput::Bytes(nbytes as u64));
    g.bench_function("concurrent", |b| {
        let pool = get_threadpool(num_cpus::get_physical());
        pool.install(|| {
            let (engine, _tmpdir) = get_bitcask();
            b.iter_batched(
                || (engine.get_handle(), kv_pairs.clone()),
                concurrent_write,
                BatchSize::SmallInput,
            );
        });
    });
    g.bench_function("sequential", |b| {
        let (engine, _tmpdir) = get_bitcask();
        b.iter_batched(
            || (engine.get_handle(), kv_pairs.clone()),
            sequential_write,
            BatchSize::SmallInput,
        );
    });
    g.finish();
}

fn bench_overwrite(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64((1 << 7) + 1);
    let kv_pairs = KeyValuePair::random_many(&mut rng, ITER, KEY_SIZE, VAL_SIZE);
    let nbytes = kv_pairs
        .iter()
        .fold(0, |acc, kv| acc + kv.0.len() + kv.1.len());

    let mut g = c.benchmark_group("bitcask_overwrite");
    g.throughput(Throughput::Bytes(nbytes as u64));
    g.bench_function("concurrent", |b| {
        get_threadpool(num_cpus::get_physical()).install(|| {
            let (engine, _tmpdir) = get_bitcask();
            let handle = engine.get_handle();
            kv_pairs.iter().cloned().for_each(|kv| {
                handle.set(kv.0, kv.1).unwrap();
            });
            b.iter_batched(
                || {
                    let mut kv_pairs = kv_pairs.to_vec();
                    kv_pairs.shuffle(&mut rand::thread_rng());
                    (handle.clone(), kv_pairs)
                },
                concurrent_write,
                BatchSize::SmallInput,
            );
        });
    });
    g.bench_function("sequential", |b| {
        let (engine, _tmpdir) = get_bitcask();
        let handle = engine.get_handle();
        kv_pairs.iter().cloned().for_each(|kv| {
            handle.set(kv.0, kv.1).unwrap();
        });
        b.iter_batched(
            || {
                let mut kv_pairs = kv_pairs.to_vec();
                kv_pairs.shuffle(&mut rand::thread_rng());
                (handle.clone(), kv_pairs)
            },
            sequential_write,
            BatchSize::SmallInput,
        );
    });
    g.finish();
}

fn bench_read(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64((1 << 7) + 1);
    let kv_pairs = KeyValuePair::random_many(&mut rng, ITER, KEY_SIZE, VAL_SIZE);
    let nbytes = kv_pairs
        .iter()
        .fold(0, |acc, kv| acc + kv.0.len() + kv.1.len());

    let mut g = c.benchmark_group("bitcask_read");
    g.throughput(Throughput::Bytes(nbytes as u64));
    g.bench_function("concurrent", |b| {
        get_threadpool(num_cpus::get_physical()).install(|| {
            let (engine, _tmpdir) = get_bitcask();
            let handle = engine.get_handle();
            kv_pairs.iter().cloned().for_each(|kv| {
                handle.set(kv.0, kv.1).unwrap();
            });
            b.iter_batched(
                || {
                    let mut kv_pairs = kv_pairs.clone();
                    kv_pairs.shuffle(&mut rand::thread_rng());
                    (engine.get_handle(), kv_pairs)
                },
                concurrent_read,
                BatchSize::SmallInput,
            );
        });
    });
    g.bench_function("sequential", |b| {
        let (engine, _tmpdir) = get_bitcask();
        let handle = engine.get_handle();
        kv_pairs.iter().cloned().for_each(|kv| {
            handle.set(kv.0, kv.1).unwrap();
        });
        b.iter_batched(
            || {
                let mut kv_pairs = kv_pairs.clone();
                kv_pairs.shuffle(&mut rand::thread_rng());
                (engine.get_handle(), kv_pairs)
            },
            sequential_read,
            BatchSize::SmallInput,
        );
    });
    g.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_write, bench_read, bench_overwrite
);
criterion_main!(benches);
