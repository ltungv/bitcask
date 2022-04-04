mod common;

use common::{
    concurrent_read_bulk_bench_iter, concurrent_write_bulk_bench_iter,
    concurrent_write_bulk_bench_iter_no_tempdir, get_bitcask, get_threadpool, rand_kv_pairs,
    sequential_read_bulk_bench_iter, sequential_write_bulk_bench_iter,
    sequential_write_bulk_bench_iter_no_tempdir,
};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use opal::engine::KeyValueStore;
use pprof::criterion::{Output, PProfProfiler};
use rand::prelude::*;

const ITER: usize = 1000;
const KEY_SIZE: usize = 64;
const VAL_SIZE: usize = 256;

pub fn bench_write(c: &mut Criterion) {
    let kv_pairs = rand_kv_pairs(ITER, KEY_SIZE, VAL_SIZE);
    let mut nbytes = 0;
    for (k, v) in kv_pairs.iter() {
        nbytes += k.len() + v.len();
    }

    let mut g = c.benchmark_group("bitcask_write");
    g.throughput(Throughput::Bytes(nbytes as u64));

    g.bench_function("concurrent", |b| {
        let pool = get_threadpool(num_cpus::get_physical());
        pool.install(|| {
            b.iter_batched(
                || {
                    let (engine, tmpdir) = get_bitcask();
                    (engine, kv_pairs.clone(), tmpdir)
                },
                concurrent_write_bulk_bench_iter,
                BatchSize::LargeInput,
            );
        });
    });
    g.bench_function("sequential", |b| {
        b.iter_batched(
            || {
                let (engine, tmpdir) = get_bitcask();
                (engine, kv_pairs.clone(), tmpdir)
            },
            sequential_write_bulk_bench_iter,
            BatchSize::LargeInput,
        );
    });
    g.finish();
}

pub fn bench_overwrite(c: &mut Criterion) {
    let kv_pairs = rand_kv_pairs(ITER, KEY_SIZE, VAL_SIZE);
    let mut nbytes = 0;
    for (k, v) in kv_pairs.iter() {
        nbytes += k.len() + v.len();
    }

    let (engine, _tmpdir) = get_bitcask();
    kv_pairs.iter().cloned().for_each(|(k, v)| {
        engine.set(k, v).unwrap();
    });

    let mut g = c.benchmark_group("bitcask_overwrite");
    g.throughput(Throughput::Bytes(nbytes as u64));

    g.bench_function("concurrent", |b| {
        let pool = get_threadpool(num_cpus::get_physical());
        pool.install(|| {
            b.iter_batched(
                || {
                    let mut kv_pairs = kv_pairs.to_vec();
                    kv_pairs.shuffle(&mut rand::thread_rng());
                    (engine.clone(), kv_pairs)
                },
                concurrent_write_bulk_bench_iter_no_tempdir,
                BatchSize::LargeInput,
            );
        });
    });
    g.bench_function("sequential", |b| {
        b.iter_batched(
            || {
                let mut kv_pairs = kv_pairs.to_vec();
                kv_pairs.shuffle(&mut rand::thread_rng());
                (engine.clone(), kv_pairs)
            },
            sequential_write_bulk_bench_iter_no_tempdir,
            BatchSize::LargeInput,
        );
    });
    g.finish();
}

pub fn bench_read(c: &mut Criterion) {
    let kv_pairs = rand_kv_pairs(ITER, KEY_SIZE, VAL_SIZE);
    let mut nbytes = 0;
    for (k, v) in kv_pairs.iter() {
        nbytes += k.len() + v.len();
    }

    let (engine, _tmpdir) = get_bitcask();
    kv_pairs.iter().cloned().for_each(|(k, v)| {
        engine.set(k, v).unwrap();
    });

    let mut g = c.benchmark_group("bitcask_read");
    g.throughput(Throughput::Bytes(nbytes as u64));

    g.bench_function("concurrent", |b| {
        let pool = get_threadpool(num_cpus::get_physical());
        pool.install(|| {
            b.iter_batched(
                || {
                    let mut kv_pairs = kv_pairs.clone();
                    kv_pairs.shuffle(&mut rand::thread_rng());
                    (engine.clone(), kv_pairs)
                },
                concurrent_read_bulk_bench_iter,
                BatchSize::LargeInput,
            );
        });
    });
    g.bench_function("sequential", |b| {
        b.iter_batched(
            || {
                let mut kv_pairs = kv_pairs.clone();
                kv_pairs.shuffle(&mut rand::thread_rng());
                (engine.clone(), kv_pairs)
            },
            sequential_read_bulk_bench_iter,
            BatchSize::LargeInput,
        );
    });
    g.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(500, Output::Flamegraph(None)));
    targets = bench_write, bench_read, bench_overwrite
);
criterion_main!(benches);
