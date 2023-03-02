use std::io::Cursor;

use bitcask::net::{connection::Connection, frame::Frame};
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use rand::{
    distributions::{Alphanumeric, Uniform},
    prelude::*,
};
use tokio::runtime::Runtime;

const ITER: usize = 10000;
struct FrameRandomizerConfig {
    array_size: usize,
    array_nesting_max: usize,
    bulk_size: usize,
    string_size: usize,
}
struct FrameRandomizer {
    rng: StdRng,
    conf: FrameRandomizerConfig,
}

impl FrameRandomizer {
    fn random(&mut self, nesting_level: usize) -> Frame {
        match self.random_size(6) {
            0 => Frame::SimpleString(self.random_string()),
            1 => Frame::Error(self.random_string()),
            2 => Frame::Integer(self.rng.gen()),
            3 => Frame::BulkString(self.random_bytes()),
            4 if nesting_level < self.conf.array_nesting_max => Frame::Array(
                (0..self.random_size(self.conf.array_size))
                    .map(|_| self.random(nesting_level + 1))
                    .collect(),
            ),
            4 | 5 => Frame::Null,
            _ => unreachable!(),
        }
    }

    fn random_size(&mut self, max: usize) -> usize {
        self.rng.sample(Uniform::new(0, max))
    }

    fn random_string(&mut self) -> String {
        let sz = self.random_size(self.conf.string_size);
        (&mut self.rng)
            .sample_iter(Alphanumeric)
            .take(sz)
            .map(char::from)
            .collect()
    }

    fn random_bytes(&mut self) -> Bytes {
        let sz = self.random_size(self.conf.bulk_size);
        (&mut self.rng).sample_iter(Alphanumeric).take(sz).collect()
    }
}

fn bench_read_frame(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let mut randomizer = FrameRandomizer {
        rng: StdRng::seed_from_u64((1 << 7) + 1),
        conf: FrameRandomizerConfig {
            array_size: 16,
            array_nesting_max: 1,
            string_size: 1024,
            bulk_size: 1024,
        },
    };

    let mut stream = Cursor::new(Vec::new());
    let mut conn = Connection::new(&mut stream);
    for _ in 0..ITER {
        runtime
            .block_on(async { conn.write_frame(&randomizer.random(0)).await })
            .unwrap();
    }

    let mut g = c.benchmark_group("read_frame");
    g.throughput(criterion::Throughput::Bytes(stream.position()));

    g.bench_function("random", |b| {
        b.to_async(&runtime).iter_batched(
            || stream.clone(),
            |mut stream| async move {
                let mut conn = Connection::new(&mut stream);
                black_box(while conn.read_frame().await.unwrap().is_some() {})
            },
            BatchSize::LargeInput,
        );
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(500, Output::Flamegraph(None)));
    targets = bench_read_frame
);
criterion_main!(benches);
