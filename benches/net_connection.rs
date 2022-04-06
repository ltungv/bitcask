mod common;

use std::io::Cursor;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use opal::net::{command, frame::Frame, connection::Connection};
use pprof::criterion::{Output, PProfProfiler};
use rand::prelude::*;
use tokio::runtime::Runtime;

use common::rand_kv_pairs;

const ITER: usize = 1000;
const KEY_SIZE: usize = 64;
const VAL_SIZE: usize = 256;

pub fn bench_read(c: &mut Criterion) {
    // A weighted distribution with 90% chance of getting 0, 5% chance of getting 1,
    // and 5% chance of getting 2.
    let weights: [u32; 3] = [90, 5, 5];
    let dist = rand::distributions::WeightedIndex::new(weights).unwrap();

    let runtime = Runtime::new().unwrap();
    let mut rng = rand::thread_rng();
    let mut stream = Cursor::new(Vec::new());
    let mut conn = Connection::new(&mut stream);

    for (k, v) in rand_kv_pairs(ITER, KEY_SIZE, VAL_SIZE) {
        let frame = match dist.sample(&mut rng) {
            // 90% GET command
            0 => Frame::from(command::Get::new(k)),
            // 5% SET command
            1 => Frame::from(command::Set::new(k, v)),
            // 5% DEL command
            2 => Frame::from(command::Del::new(vec![k, v])),

            _ => unreachable!(),
        };
        runtime
            .block_on(async { conn.write_frame(&frame).await })
            .unwrap();
    }
    drop(runtime);

    let mut g = c.benchmark_group("parse_random");
    g.throughput(Throughput::Bytes(stream.get_ref().len() as u64));

    g.bench_with_input(
        BenchmarkId::new("ratio_10:90", stream.get_ref().len()),
        &stream,
        |b, s| {
            b.to_async(Runtime::new().unwrap()).iter_batched(
                || Connection::new(s.clone()),
                |mut conn| async move {
                    conn.read_frame().await.unwrap();
                },
                BatchSize::SmallInput,
            );
        },
    );
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_read
);
criterion_main!(benches);
