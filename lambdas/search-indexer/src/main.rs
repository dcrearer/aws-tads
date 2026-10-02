//! Search-indexer Lambda (DynamoDB-Streams-triggered) — projects TADS metadata into OpenSearch.
//!
//! This is what delivers TADS's "search by the data itself" pillar. Phase 2: idempotently
//! upsert events/tracks/schemas into OpenSearch Serverless (indexing payload content, tags,
//! timerange/`start_ns`/`end_ns`, `schema_id`, `timeline_reference.entity_id`) and tombstone
//! deletes. It is strictly downstream of the system-of-record and can be rebuilt from a replay.
//! Phase 1 is a stub.

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;

#[tracing::instrument(skip_all, fields(handler = "search_indexer"))]
async fn handler(event: LambdaEvent<Value>) -> Result<(), Error> {
    let records = event
        .payload
        .get("Records")
        .and_then(Value::as_array)
        .map(|r| r.len())
        .unwrap_or(0);
    tracing::info!(records, "search-indexer invoked (Phase 2 stub)");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();
    lambda_runtime::run(service_fn(handler)).await
}
