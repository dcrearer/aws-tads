//! CPU hot-path benchmarks for the domain layer.
//!
//! These measure the *local* per-event work the events handler does. They cannot measure
//! DynamoDB latency or partition throughput (that needs a load test against a real table) — in
//! fact the overlap bench exists to show the in-memory part is negligible, so the real cost of
//! the overlap check is the DynamoDB query, not the CPU.
//!
//! The headline is `validate/compile_each_event` vs `validate/reuse_validator`: our
//! `validate_payload` compiles the JSON Schema on every call, so a batched POST recompiles the
//! same schema once per event. The gap between the two benches is the cost that hoisting the
//! validator out of the per-event loop would remove.

use std::fs;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use serde_json::{Value, json};
use tads_domain::{TadsId, TimeRange, TrackEvent, validate_payload};

fn subtitle_schema() -> Value {
    let path = format!("{}/tests/fixtures/schemas.json", env!("CARGO_MANIFEST_DIR"));
    let schemas: Vec<Value> = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    schemas
        .into_iter()
        .find(|s| s["external_id"] == "phrame.compliance-recorder.subtitle-text.v1")
        .expect("subtitle schema present")["schema"]
        .clone()
}

fn bench_timerange(c: &mut Criterion) {
    let inputs = [
        "_",
        "()",
        "[1694429247:0]",
        "[0:0_10:0)",
        "(5:0_",
        "[1791293599:812856841_1791293599:812856842)",
    ];
    c.bench_function("timerange/parse", |b| {
        b.iter(|| {
            for s in &inputs {
                let _ = black_box(s.parse::<TimeRange>().unwrap());
            }
        })
    });

    let parsed: Vec<TimeRange> = inputs.iter().map(|s| s.parse().unwrap()).collect();
    c.bench_function("timerange/format", |b| {
        b.iter(|| {
            for tr in &parsed {
                let _ = black_box(tr.to_string());
            }
        })
    });
}

fn bench_validation(c: &mut Criterion) {
    let schema = subtitle_schema();
    let payload = json!({"text": "ORDER, ORDER", "pid": 5, "dedup_window_seconds": 60});

    // Current behavior: validate_payload compiles the schema every call.
    c.bench_function("validate/compile_each_event", |b| {
        b.iter(|| black_box(validate_payload(black_box(&payload), black_box(&schema)).is_ok()))
    });

    // Hoisted: compile the validator once, reuse it per event (what a batch POST should do).
    let validator = jsonschema::validator_for(&schema).unwrap();
    c.bench_function("validate/reuse_validator", |b| {
        b.iter(|| black_box(validator.is_valid(black_box(&payload))))
    });
}

fn bench_overlap_filter(c: &mut Criterion) {
    // The in-memory part of the allow_overlaps / editable_events check: scan a page of events
    // comparing ids. This is what the handler does over a query result.
    for n in [100usize, 1000] {
        let events: Vec<TrackEvent> = (0..n)
            .map(|i| TrackEvent {
                id: TadsId::new_v4(),
                timerange: format!("[{i}:0_{}:0)", i + 1).parse().unwrap(),
                payload: json!({}),
                created: None,
            })
            .collect();
        let target = events[n / 2].id;
        c.bench_function(&format!("overlap/id_scan_{n}"), |b| {
            b.iter(|| {
                let edits = events.iter().any(|e| e.id == black_box(target));
                let overlaps = events.iter().any(|e| e.id != black_box(target));
                black_box((edits, overlaps))
            })
        });
    }
}

criterion_group!(
    benches,
    bench_timerange,
    bench_validation,
    bench_overlap_filter
);
criterion_main!(benches);
