//! Webhook-dispatcher Lambda (DynamoDB-Streams-triggered) — delivers event notifications.
//!
//! Phase 2: match each change record against webhook subscription filters (`schema_ids`,
//! `track_ids`, `timeline_reference.entity_id`, `editable_events`, `tags`), POST the relevant
//! event type to the subscriber `url` with the `api_key_name` header, retry, and drive the
//! webhook `status` to `error` on repeated failure. Phase 1 is a stub.

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;

#[tracing::instrument(skip_all, fields(handler = "webhook_dispatcher"))]
async fn handler(event: LambdaEvent<Value>) -> Result<(), Error> {
    let records = event
        .payload
        .get("Records")
        .and_then(Value::as_array)
        .map(|r| r.len())
        .unwrap_or(0);
    tracing::info!(records, "webhook-dispatcher invoked (Phase 2 stub)");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();
    lambda_runtime::run(service_fn(handler)).await
}
