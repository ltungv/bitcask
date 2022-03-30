mod common;

use common::{get_lfs, get_threadpool, prebuilt_kv_pairs};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use opal::engine::KeyValueStore;
use pprof::criterion::{Output, PProfProfiler};
use rand::prelude::*;

const ITER: usize = 1000;
const KEY_SIZE: usize = 1000;
const VAL_SIZE: usize = 1000;

pub fn bench_write(c: &mut Criterion) {
    let kv_pairs = prebuilt_kv_pairs(ITER, KEY_SIZE, VAL_SIZE);
    let pool = get_threadpool(num_cpus::get_physical());

    c.bench_function("lfs_concurrent_write", |b| {
        pool.install(|| {
            b.iter_batched(
                || {
                    let (engine, tmpdir) = get_lfs();
                    (engine, kv_pairs.clone(), tmpdir)
                },
                |(engine, kv_pairs, _tmpdir)| {
                    rayon::scope(move |s| {
                        kv_pairs.into_iter().for_each(|(k, v)| {
                            let engine = engine.clone();
                            s.spawn(move |_| engine.set(black_box(k), black_box(v)).unwrap());
                        });
                    });
                },
                BatchSize::LargeInput,
            )
        });
    });
}

pub fn bench_read(c: &mut Criterion) {
    let kv_pairs = prebuilt_kv_pairs(ITER, KEY_SIZE, VAL_SIZE);
    let pool = get_threadpool(num_cpus::get_physical());

    let (engine, _tmpdir) = get_lfs();
    kv_pairs
        .iter()
        .cloned()
        .for_each(|(k, v)| engine.set(k, v).unwrap());

    c.bench_function("lfs_concurrent_read", |b| {
        pool.install(|| {
            b.iter_batched(
                || {
                    let mut kv_pairs = kv_pairs.clone();
                    kv_pairs.shuffle(&mut rand::thread_rng());
                    (engine.clone(), kv_pairs)
                },
                |(engine, kv_pairs)| {
                    rayon::scope(move |s| {
                        kv_pairs.into_iter().for_each(|(k, v)| {
                            let engine = engine.clone();
                            s.spawn(move |_| assert_eq!(v, engine.get(black_box(&k)).unwrap()));
                        });
                    })
                },
                BatchSize::LargeInput,
            )
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_write, bench_read
);
criterion_main!(benches);
