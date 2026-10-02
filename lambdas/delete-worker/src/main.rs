//! Delete-worker Lambda (SQS-triggered) — processes async track delete requests.
//!
//! Phase 2: walk each request `created → started → done|error`, delete matching Events in
//! batches (updating `timerange_remaining`), delete the Track if `delete_track`, and fire the
//! corresponding webhooks. Phase 1 is a stub that acknowledges the batch.

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;

#[tracing::instrument(skip_all, fields(handler = "delete_worker"))]
async fn handler(event: LambdaEvent<Value>) -> Result<(), Error> {
    let records = event
        .payload
        .get("Records")
        .and_then(Value::as_array)
        .map(|r| r.len())
        .unwrap_or(0);
    tracing::info!(records, "delete-worker invoked (Phase 2 stub)");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();
    lambda_runtime::run(service_fn(handler)).await
}
