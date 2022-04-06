mod common;

use std::io::Cursor;

use bytes::{Buf, Bytes};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use opal::net::{connection::Connection, frame::Frame};
use pprof::criterion::{Output, PProfProfiler};
use rand::{distributions::Alphanumeric, prelude::*};
use tokio::runtime::Runtime;

const ITER: usize = 10000;

const ARRAY_LEN: usize = 5;
const ARRAY_NEST: usize = 1;
const STRING_SIZE: usize = 64;
const BULK_SIZE: usize = 256;

fn rand_frame(nest: usize) -> Frame {
    let mut rng = rand::thread_rng();
    match (0..6).choose(&mut rng).unwrap() {
        0 => Frame::SimpleString(
            rng.sample_iter(Alphanumeric)
                .take((0..STRING_SIZE).choose(&mut rand::thread_rng()).unwrap())
                .map(char::from)
                .collect(),
        ),
        1 => Frame::Error(
            rng.sample_iter(Alphanumeric)
                .take((0..STRING_SIZE).choose(&mut rand::thread_rng()).unwrap())
                .map(char::from)
                .collect(),
        ),
        2 => Frame::Integer(rand::random::<i64>()),
        3 => Frame::BulkString(
            rng.sample_iter(Alphanumeric)
                .take((0..BULK_SIZE).choose(&mut rand::thread_rng()).unwrap())
                .collect(),
        ),
        4 => {
            if nest == ARRAY_NEST {
                Frame::Null
            } else {
                Frame::Array(
                    (0..(0..ARRAY_LEN).choose(&mut rand::thread_rng()).unwrap())
                        .map(|_| rand_frame(nest + 1))
                        .collect(),
                )
            }
        }
        5 => Frame::Null,
        _ => unreachable!(),
    }
}

pub fn bench_parse(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let mut stream = Cursor::new(Vec::new());
    let mut conn = Connection::new(&mut stream);
    for _ in 0..ITER {
        runtime
            .block_on(async { conn.write_frame(&rand_frame(0)).await })
            .unwrap();
    }
    drop(runtime);

    let mut g = c.benchmark_group("parse_random");
    g.throughput(Throughput::Bytes(stream.get_ref().len() as u64));

    g.bench_with_input("ratio_10:90", &stream, |b, s| {
        b.iter_batched(
            || Bytes::from(s.get_ref().clone()),
            |mut s| {
                while s.has_remaining() {
                    let mut buf = Cursor::new(&s[..]);
                    match Frame::check(&mut buf) {
                        Ok(()) => {
                            // Get the byte length of the frame
                            let len = buf.position() as usize;

                            // Parse the frame
                            buf.set_position(0);
                            Frame::parse(&mut buf).unwrap();

                            // Discard the frame from the buffer
                            s.advance(len);
                        }
                        _ => unreachable!(),
                    }
                }
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(500, Output::Flamegraph(None)));
    targets = bench_parse
);
criterion_main!(benches);
