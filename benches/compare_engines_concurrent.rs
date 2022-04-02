mod common;

use common::{get_bitcask, get_dashmap, get_sled, get_threadpool, prebuilt_kv_pairs};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Bencher, BenchmarkId, Criterion, Throughput
};
use opal::engine::{self, KeyValueStore};
use pprof::criterion::{Output, PProfProfiler};
use rand::prelude::*;
use rayon::ThreadPoolBuilder;
use tempfile::TempDir;

const ITER: usize = 1000;
const KEY_SIZE: usize = 1000;
const VAL_SIZE: usize = 1000;

pub fn bench_write(c: &mut Criterion) {
    let phys_cpus = num_cpus::get_physical();
    let kv_pairs = prebuilt_kv_pairs(ITER, KEY_SIZE, VAL_SIZE);
    let mut nbytes = 0;
    for (k, v) in kv_pairs.iter() {
        nbytes += k.len() + v.len();
    }

    let mut g = c.benchmark_group("compare_engines_concurrent_write");
    g.throughput(Throughput::Bytes(nbytes as u64));

    g.bench_with_input(
        BenchmarkId::new("bitcask", phys_cpus),
        &(&kv_pairs, engine::Type::BitCask, phys_cpus),
        concurrent_write_bulk_bench,
    );
    g.bench_with_input(
        BenchmarkId::new("sled", phys_cpus),
        &(&kv_pairs, engine::Type::Sled, phys_cpus),
        concurrent_write_bulk_bench,
    );
    g.bench_with_input(
        BenchmarkId::new("dashmap", phys_cpus),
        &(&kv_pairs, engine::Type::DashMap, phys_cpus),
        concurrent_write_bulk_bench,
    );
    g.finish();
}

fn concurrent_write_bulk_bench(
    b: &mut Bencher,
    (kv_pairs, engine, nthreads): &(&Vec<(Vec<u8>, Vec<u8>)>, engine::Type, usize),
) {
    let pool = ThreadPoolBuilder::new()
        .num_threads(*nthreads)
        .build()
        .unwrap();

    match *engine {
        engine::Type::BitCask => {
            pool.install(|| {
                b.iter_batched(
                    || {
                        let (engine, tmpdir) = get_bitcask();
                        (engine, kv_pairs.to_vec(), tmpdir)
                    },
                    concurrent_write_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
        engine::Type::Sled => {
            pool.install(|| {
                b.iter_batched(
                    || {
                        let (engine, tmpdir) = get_sled();
                        (engine, kv_pairs.to_vec(), tmpdir)
                    },
                    concurrent_write_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
        engine::Type::DashMap => {
            pool.install(|| {
                b.iter_batched(
                    || {
                        let (engine, tmpdir) = get_dashmap();
                        (engine, kv_pairs.to_vec(), tmpdir)
                    },
                    concurrent_write_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
    }
}

fn concurrent_write_bulk_bench_iter<E>(
    (engine, kv_pairs, _tmpdir): (E, Vec<(Vec<u8>, Vec<u8>)>, TempDir),
) where
    E: KeyValueStore,
{
    rayon::scope(move |s| {
        kv_pairs.into_iter().for_each(|(k, v)| {
            let engine = engine.clone();
            s.spawn(move |_| {
                engine.set(black_box(&k), black_box(&v)).unwrap();
            });
        });
    });
}

pub fn bench_read(c: &mut Criterion) {
    let phys_cpus = num_cpus::get_physical();
    let kv_pairs = prebuilt_kv_pairs(ITER, KEY_SIZE, VAL_SIZE);
    let mut nbytes = 0;
    for (k, v) in kv_pairs.iter() {
        nbytes += k.len() + v.len();
    }

    let mut g = c.benchmark_group("compare_engines_concurrent_read");
    g.throughput(Throughput::Bytes(nbytes as u64));

    g.bench_with_input(
        BenchmarkId::new("bitcask", phys_cpus),
        &(&kv_pairs, engine::Type::BitCask, phys_cpus),
        concurrent_read_bulk_bench,
    );
    g.bench_with_input(
        BenchmarkId::new("sled", phys_cpus),
        &(&kv_pairs, engine::Type::Sled, phys_cpus),
        concurrent_read_bulk_bench,
    );
    g.bench_with_input(
        BenchmarkId::new("dashmap", phys_cpus),
        &(&kv_pairs, engine::Type::DashMap, phys_cpus),
        concurrent_read_bulk_bench,
    );
    g.finish();
}

fn concurrent_read_bulk_bench(
    b: &mut Bencher,
    (kv_pairs, engine, nthreads): &(&Vec<(Vec<u8>, Vec<u8>)>, engine::Type, usize),
) {
    let pool = get_threadpool(*nthreads);

    let mut rng = StdRng::from_seed([0u8; 32]);
    match *engine {
        engine::Type::BitCask => {
            let (engine, _tmpdir) = get_bitcask();
            kv_pairs.iter().for_each(|(k, v)| {
                engine.set(k, v).unwrap();
            });

            pool.install(move || {
                b.iter_batched(
                    || {
                        let mut kv_pairs = kv_pairs.to_vec();
                        kv_pairs.shuffle(&mut rng);
                        (engine.clone(), kv_pairs)
                    },
                    concurrent_read_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
        engine::Type::Sled => {
            let (engine, _tmpdir) = get_sled();
            kv_pairs.iter().for_each(|(k, v)| {
                engine.set(k, v).unwrap();
            });

            pool.install(move || {
                b.iter_batched(
                    || {
                        let mut kv_pairs = kv_pairs.to_vec();
                        kv_pairs.shuffle(&mut rng);
                        (engine.clone(), kv_pairs)
                    },
                    concurrent_read_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
        engine::Type::DashMap => {
            let (engine, _tmpdir) = get_dashmap();
            kv_pairs.iter().for_each(|(k, v)| {
                engine.set(k, v).unwrap();
            });

            pool.install(move || {
                b.iter_batched(
                    || {
                        let mut kv_pairs = kv_pairs.to_vec();
                        kv_pairs.shuffle(&mut rng);
                        (engine.clone(), kv_pairs)
                    },
                    concurrent_read_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
    }
}

fn concurrent_read_bulk_bench_iter<E>((engine, kv_pairs): (E, Vec<(Vec<u8>, Vec<u8>)>))
where
    E: KeyValueStore,
{
    rayon::scope(move |s| {
        kv_pairs.into_iter().for_each(|(k, _)| {
            let engine = engine.clone();
            s.spawn(move |_| {
                engine.get(black_box(&k)).unwrap();
            });
        });
    })
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_write, bench_read
);
criterion_main!(benches);
