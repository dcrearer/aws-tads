// Deep reqwest + AWS SDK + lambda_runtime future nesting exceeds the default type-layout limit.
#![recursion_limit = "512"]
//! vu-analysis-worker Lambda (SQS-triggered, out-of-VPC).
//!
//! Builds the TAMS/Bedrock/TADS edges once at cold start, then processes each SQS record
//! (a `flows/segments_added` payload) through the [`Worker`]. Returns `batchItemFailures` so SQS
//! retries only failed messages and routes exhausted ones to the DLQ.

use std::sync::Arc;

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::{Value, json};
use tads_lambda_vu_analysis_worker::{DefaultWorker, WorkerConfig, build_worker};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();
    let cfg = WorkerConfig::load().map_err(|e| Error::from(e.to_string()))?;
    let worker = Arc::new(build_worker(cfg).await.map_err(Error::from)?);

    lambda_runtime::run(service_fn(move |event: LambdaEvent<Value>| {
        let worker = worker.clone();
        async move { handle(&worker, event).await }
    }))
    .await
}

#[tracing::instrument(skip_all, fields(handler = "vu_analysis_worker"))]
async fn handle(worker: &DefaultWorker, event: LambdaEvent<Value>) -> Result<Value, Error> {
    let records = event
        .payload
        .get("Records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut failures = Vec::new();
    for record in &records {
        let msg_id = record
            .get("messageId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let Some(body) = record.get("body").and_then(Value::as_str) else {
            tracing::warn!(message_id = %msg_id, "SQS record missing string body; skipping");
            continue;
        };
        if let Err(e) = worker.process_message(body).await {
            tracing::warn!(message_id = %msg_id, error = %e, "analysis failed; SQS will retry");
            failures.push(json!({ "itemIdentifier": msg_id }));
        }
    }
    Ok(json!({ "batchItemFailures": failures }))
}
