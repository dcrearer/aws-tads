//! CPU hot-path benchmarks for the store's timerange/cursor encoding.
//!
//! CPU-only: these are the per-event/per-page encodings on the write and list paths. Postgres
//! query latency and GiST index behaviour are not measurable here (that needs a load test against
//! a real Aurora cluster).

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use serde_json::json;
use tads_domain::TimeRange;
use tads_store::cursor::{decode_cursor, encode_cursor};
use tads_store::keys::{end_ns, pg_range, start_ns};

fn bench_keys(c: &mut Criterion) {
    let tr: TimeRange = "[1791293599:812856841_1791293609:0)".parse().unwrap();
    c.bench_function("keys/pg_range", |b| {
        b.iter(|| black_box(pg_range(black_box(&tr))))
    });
    c.bench_function("keys/start_end_ns", |b| {
        b.iter(|| black_box((start_ns(black_box(&tr)), end_ns(black_box(&tr)))))
    });
}

fn bench_cursor(c: &mut Criterion) {
    let key = json!({
        "start_ns": 1_791_293_599_812_856_841_i64,
        "event_id": "2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f",
    });

    c.bench_function("cursor/encode", |b| {
        b.iter(|| black_box(encode_cursor(black_box(&key))))
    });

    let encoded = encode_cursor(&key);
    c.bench_function("cursor/decode", |b| {
        b.iter(|| black_box(decode_cursor(black_box(&encoded)).unwrap()))
    });
}

criterion_group!(benches, bench_keys, bench_cursor);
criterion_main!(benches);
