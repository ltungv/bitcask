mod common;

use bitcask::storage::KeyValueStorage;
use common::{
    concurrent_read_bulk_bench_iter, concurrent_write_bulk_bench_iter, get_bitcask, get_sled,
    get_threadpool, rand_kv_pairs, sequential_read_bulk_bench_iter,
    sequential_write_bulk_bench_iter, EngineType, KeyValuePair,
};
use criterion::{criterion_group, criterion_main, BatchSize, Bencher, Criterion, Throughput};
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
        &(&kv_pairs, EngineType::Bitcask),
        concurrent_write_bulk_bench,
    );
    g.bench_with_input(
        "sled",
        &(&kv_pairs, EngineType::Sled),
        concurrent_write_bulk_bench,
    );
    g.finish();

    let mut g = c.benchmark_group("compare_engines/sequential_write");
    g.throughput(Throughput::Bytes(nbytes as u64));

    g.bench_with_input(
        "bitcask",
        &(&kv_pairs, EngineType::Bitcask),
        sequential_write_bulk_bench,
    );
    g.bench_with_input(
        "sled",
        &(&kv_pairs, EngineType::Sled),
        sequential_write_bulk_bench,
    );
    g.finish();
}

fn concurrent_write_bulk_bench(
    b: &mut Bencher,
    (kv_pairs, engine): &(&Vec<KeyValuePair>, EngineType),
) {
    let pool = get_threadpool(num_cpus::get_physical());
    match *engine {
        EngineType::Bitcask => {
            let (engine, _tmpdir) = get_bitcask();
            pool.install(|| {
                b.iter_batched(
                    || (engine.get_handle(), kv_pairs.to_vec()),
                    concurrent_write_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
        EngineType::Sled => {
            pool.install(|| {
                b.iter_batched(
                    || {
                        let (engine, _tmpdir) = get_sled();
                        (engine, kv_pairs.to_vec())
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
    (kv_pairs, engine): &(&Vec<KeyValuePair>, EngineType),
) {
    match *engine {
        EngineType::Bitcask => {
            let (engine, _tmpdir) = get_bitcask();
            b.iter_batched(
                || (engine.get_handle(), kv_pairs.to_vec()),
                sequential_write_bulk_bench_iter,
                BatchSize::SmallInput,
            );
        }
        EngineType::Sled => {
            let (engine, _tmpdir) = get_sled();
            b.iter_batched(
                || (engine.clone(), kv_pairs.to_vec()),
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
        &(&kv_pairs, EngineType::Bitcask),
        concurrent_read_bulk_bench,
    );
    g.bench_with_input(
        "sled",
        &(&kv_pairs, EngineType::Sled),
        concurrent_read_bulk_bench,
    );
    g.finish();

    let mut g = c.benchmark_group("compare_engines/sequential_read");
    g.throughput(Throughput::Bytes(nbytes as u64));

    {
        let (engine, _tmpdir) = get_bitcask();
        g.bench_with_input(
            "bitcask",
            &(&kv_pairs, engine.get_handle()),
            sequential_read_bulk_bench,
        );
    }
    {
        let (engine, _tmpdir) = get_sled();
        g.bench_with_input("sled", &(&kv_pairs, engine), sequential_read_bulk_bench);
    }
    g.finish();
}

fn concurrent_read_bulk_bench(
    b: &mut Bencher,
    (kv_pairs, engine): &(&Vec<KeyValuePair>, EngineType),
) {
    let pool = get_threadpool(num_cpus::get_physical());
    let mut rng = StdRng::from_seed([0u8; 32]);
    match *engine {
        EngineType::Bitcask => {
            let (engine, _tmpdir) = get_bitcask();
            let handle = engine.get_handle();
            kv_pairs.iter().for_each(|(k, v)| {
                handle.set(k.clone(), v.clone()).unwrap();
            });
            pool.install(move || {
                b.iter_batched(
                    || {
                        let mut kv_pairs = kv_pairs.to_vec();
                        kv_pairs.shuffle(&mut rng);
                        (engine.get_handle(), kv_pairs)
                    },
                    concurrent_read_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
        EngineType::Sled => {
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
    }
}

fn sequential_read_bulk_bench<E>(b: &mut Bencher, (kv_pairs, engine): &(&Vec<KeyValuePair>, E))
where
    E: KeyValueStorage,
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
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_write, bench_read
);
criterion_main!(benches);
