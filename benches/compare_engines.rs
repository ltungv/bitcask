mod common;

use common::{
    concurrent_read_bulk_bench_iter, concurrent_write_bulk_bench_iter, get_bitcask, get_dashmap,
    get_sled, get_threadpool, rand_kv_pairs, sequential_read_bulk_bench_iter,
    sequential_write_bulk_bench_iter, KeyValuePair,
};
use criterion::{criterion_group, criterion_main, BatchSize, Bencher, Criterion, Throughput};
use opal::engine::{self, KeyValueStore};
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

    let mut g = c.benchmark_group("compare_engines/concurrent_write");
    g.throughput(Throughput::Bytes(nbytes as u64));

    g.bench_with_input(
        "bitcask",
        &(&kv_pairs, engine::Type::BitCask),
        concurrent_write_bulk_bench,
    );
    g.bench_with_input(
        "sled",
        &(&kv_pairs, engine::Type::Sled),
        concurrent_write_bulk_bench,
    );
    g.bench_with_input(
        "dashmap",
        &(&kv_pairs, engine::Type::DashMap),
        concurrent_write_bulk_bench,
    );
    g.finish();

    let mut g = c.benchmark_group("compare_engines/sequential_write");
    g.throughput(Throughput::Bytes(nbytes as u64));

    g.bench_with_input(
        "bitcask",
        &(&kv_pairs, engine::Type::BitCask),
        sequential_write_bulk_bench,
    );
    g.bench_with_input(
        "sled",
        &(&kv_pairs, engine::Type::Sled),
        sequential_write_bulk_bench,
    );
    g.bench_with_input(
        "dashmap",
        &(&kv_pairs, engine::Type::DashMap),
        sequential_write_bulk_bench,
    );
    g.finish();
}

fn concurrent_write_bulk_bench(
    b: &mut Bencher,
    (kv_pairs, engine): &(&Vec<KeyValuePair>, engine::Type),
) {
    let pool = get_threadpool(num_cpus::get_physical());
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

fn sequential_write_bulk_bench(
    b: &mut Bencher,
    (kv_pairs, engine): &(&Vec<KeyValuePair>, engine::Type),
) {
    match *engine {
        engine::Type::BitCask => {
            b.iter_batched(
                || {
                    let (engine, tmpdir) = get_bitcask();
                    (engine, kv_pairs.to_vec(), tmpdir)
                },
                sequential_write_bulk_bench_iter,
                BatchSize::SmallInput,
            );
        }
        engine::Type::Sled => {
            b.iter_batched(
                || {
                    let (engine, tmpdir) = get_sled();
                    (engine, kv_pairs.to_vec(), tmpdir)
                },
                sequential_write_bulk_bench_iter,
                BatchSize::SmallInput,
            );
        }
        engine::Type::DashMap => {
            b.iter_batched(
                || {
                    let (engine, tmpdir) = get_dashmap();
                    (engine, kv_pairs.to_vec(), tmpdir)
                },
                sequential_write_bulk_bench_iter,
                BatchSize::SmallInput,
            );
        }
    }
}

pub fn bench_read(c: &mut Criterion) {
    let kv_pairs = rand_kv_pairs(ITER, KEY_SIZE, VAL_SIZE);
    let mut nbytes = 0;
    for (k, v) in kv_pairs.iter() {
        nbytes += k.len() + v.len();
    }

    let mut g = c.benchmark_group("compare_engines/concurrent_read");
    g.throughput(Throughput::Bytes(nbytes as u64));

    g.bench_with_input(
        "bitcask",
        &(&kv_pairs, engine::Type::BitCask),
        concurrent_read_bulk_bench,
    );
    g.bench_with_input(
        "sled",
        &(&kv_pairs, engine::Type::Sled),
        concurrent_read_bulk_bench,
    );
    g.bench_with_input(
        "dashmap",
        &(&kv_pairs, engine::Type::DashMap),
        concurrent_read_bulk_bench,
    );
    g.finish();

    let mut g = c.benchmark_group("compare_engines/sequential_read");
    g.throughput(Throughput::Bytes(nbytes as u64));

    {
        let (engine, _tmpdir) = get_bitcask();
        g.bench_with_input(
            "bitcask",
            &(&kv_pairs, engine),
            sequential_read_bulk_bench,
        );
    }
    {
        let (engine, _tmpdir) = get_sled();
        g.bench_with_input(
            "sled",
            &(&kv_pairs, engine),
            sequential_read_bulk_bench,
        );
    }
    {
        let (engine, _tmpdir) = get_dashmap();
        g.bench_with_input(
            "dashmap",
            &(&kv_pairs, engine),
            sequential_read_bulk_bench,
        );
    }
    g.finish();
}

fn concurrent_read_bulk_bench(
    b: &mut Bencher,
    (kv_pairs, engine): &(&Vec<KeyValuePair>, engine::Type),
) {
    let pool = get_threadpool(num_cpus::get_physical());
    let mut rng = StdRng::from_seed([0u8; 32]);
    match *engine {
        engine::Type::BitCask => {
            let (engine, _tmpdir) = get_bitcask();
            kv_pairs.iter().for_each(|(k, v)| {
                engine.set(k.clone(), v.clone()).unwrap();
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
            kv_pairs.iter().cloned().for_each(|(k, v)| {
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
            kv_pairs.iter().cloned().for_each(|(k, v)| {
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

fn sequential_read_bulk_bench<E>(b: &mut Bencher, (kv_pairs, engine): &(&Vec<KeyValuePair>, E))
where
    E: KeyValueStore,
{
    kv_pairs.iter().cloned().for_each(|(k, v)| {
        engine.set(k, v).unwrap();
    });

    b.iter_batched(
        || {
            let mut kv_pairs = kv_pairs.to_vec();
            kv_pairs.shuffle(&mut rand::thread_rng());
            (engine.clone(), kv_pairs)
        },
        sequential_read_bulk_bench_iter,
        BatchSize::SmallInput,
    );
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(500, Output::Flamegraph(None)));
    targets = bench_write, bench_read
);
criterion_main!(benches);
