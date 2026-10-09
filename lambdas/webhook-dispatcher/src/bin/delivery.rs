//! Webhook delivery worker (out-of-VPC, SQS-triggered).
//!
//! POSTs each [`DeliveryJob`] callback to the subscriber `url` with its optional api-key header and
//! a stable `x-tads-event-id` (for subscriber-side dedupe). No database access — the job is
//! self-contained. Runs **outside the VPC** because subscribers are public HTTPS endpoints (the
//! isolated VPC has no NAT). Reports per-message failures (`batchItemFailures`) so SQS retries only
//! the failed messages (visibility backoff) and routes exhausted ones to the DLQ.

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::{Value, json};
use tads_lambda_webhook_dispatcher::DeliveryJob;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();
    let http = reqwest::Client::builder()
        .build()
        .map_err(|e| Error::from(e.to_string()))?;
    lambda_runtime::run(service_fn(move |event: LambdaEvent<Value>| {
        let http = http.clone();
        async move { handle(&http, event).await }
    }))
    .await
}

#[tracing::instrument(skip_all, fields(handler = "webhook_delivery"))]
async fn handle(http: &reqwest::Client, event: LambdaEvent<Value>) -> Result<Value, Error> {
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
        if let Err(e) = deliver_record(http, record).await {
            tracing::warn!(message_id = %msg_id, error = %e, "delivery failed; SQS will retry");
            failures.push(json!({ "itemIdentifier": msg_id }));
        }
    }
    // Partial-batch response: only failed messages become visible again.
    Ok(json!({ "batchItemFailures": failures }))
}

async fn deliver_record(http: &reqwest::Client, record: &Value) -> Result<(), String> {
    let body_str = record
        .get("body")
        .and_then(Value::as_str)
        .ok_or("SQS record missing string body")?;
    let job: DeliveryJob =
        serde_json::from_str(body_str).map_err(|e| format!("invalid DeliveryJob: {e}"))?;

    let mut req = http
        .post(&job.url)
        .json(&job.body)
        .header("x-tads-event-id", &job.event_id);
    if let (Some(name), Some(value)) = (&job.api_key_name, &job.api_key_value) {
        req = req.header(name, value);
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if resp.status().is_success() {
        tracing::info!(url = %job.url, webhook_id = %job.webhook_id, "delivered");
        Ok(())
    } else {
        Err(format!("subscriber returned {}", resp.status()))
    }
}
