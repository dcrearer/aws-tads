//! CPU hot-path benchmarks for the store's key/cursor encoding.
//!
//! These are the per-event/per-page encodings on the write and list paths. They are CPU-only;
//! DynamoDB request latency and per-partition throughput are not measurable here (hot-partition
//! behavior needs a load test against a real table).

use std::collections::HashMap;
use std::hint::black_box;

use aws_sdk_dynamodb::types::AttributeValue;
use criterion::{Criterion, criterion_group, criterion_main};
use tads_store::cursor::{decode_cursor, encode_cursor};
use tads_store::keys::{encode_sortable, event_sort_key};

fn bench_keys(c: &mut Criterion) {
    c.bench_function("keys/encode_sortable", |b| {
        b.iter(|| black_box(encode_sortable(black_box(1_791_293_599_812_856_841_i128))))
    });
    c.bench_function("keys/event_sort_key", |b| {
        b.iter(|| {
            black_box(event_sort_key(
                black_box(1_791_293_599_812_856_841_i128),
                black_box("2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f"),
            ))
        })
    });
}

fn bench_cursor(c: &mut Criterion) {
    let mut key = HashMap::new();
    key.insert(
        "track_id".to_string(),
        AttributeValue::S("track-1".to_string()),
    );
    key.insert(
        "sk".to_string(),
        AttributeValue::S(
            "7fffffffe64e9d0f2d3b7c49#2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f".to_string(),
        ),
    );

    c.bench_function("cursor/encode", |b| {
        b.iter(|| black_box(encode_cursor(black_box(&key)).unwrap()))
    });

    let encoded = encode_cursor(&key).unwrap();
    c.bench_function("cursor/decode", |b| {
        b.iter(|| black_box(decode_cursor(black_box(&encoded)).unwrap()))
    });
}

criterion_group!(benches, bench_keys, bench_cursor);
criterion_main!(benches);
