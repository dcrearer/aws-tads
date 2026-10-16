//! Webhook delivery worker (out-of-VPC, SQS-triggered).
//!
//! POSTs each [`DeliveryJob`] callback to the subscriber `url` with its optional api-key header and
//! a stable `x-tads-event-id` (for subscriber-side dedupe). No database access — the job is
//! self-contained. Runs **outside the VPC** because subscribers are public HTTPS endpoints (the
//! isolated VPC has no NAT). Reports per-message failures (`batchItemFailures`) so SQS retries only
//! the failed messages (visibility backoff) and routes exhausted ones to the DLQ.

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::{Value, json};
use tads_lambda_webhook::DeliveryJob;

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

    // SSRF guard (delivery-time): re-resolve the host and refuse if any address is private/
    // link-local/loopback/metadata. Registration already validated the URL, but DNS can change
    // between registration and delivery (rebinding), so we re-check every resolved address here.
    ensure_safe_destination(&job.url).await?;

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

/// Reject the destination unless it is an https URL that resolves exclusively to public,
/// non-blocked addresses. Residual TOCTOU: reqwest re-resolves on connect, so a determined
/// rebinding attacker could still race us; pinning the socket to the validated address would close
/// that, at the cost of managing TLS SNI/Host manually. For a webhook callback this check is a
/// proportionate control and strictly better than trusting the stored URL.
async fn ensure_safe_destination(raw: &str) -> Result<(), String> {
    let url = url::Url::parse(raw).map_err(|e| format!("invalid url `{raw}`: {e}"))?;
    if url.scheme() != "https" {
        return Err(format!("refusing non-https destination `{raw}`"));
    }
    let host = url
        .host_str()
        .ok_or_else(|| format!("url `{raw}` has no host"))?;
    let port = url.port_or_known_default().unwrap_or(443);

    let addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| format!("dns resolution of `{host}` failed: {e}"))?;
    let mut resolved = false;
    for addr in addrs {
        resolved = true;
        if tads_domain::is_blocked_ip(addr.ip()) {
            return Err(format!(
                "refusing delivery: `{host}` resolves to blocked address {}",
                addr.ip()
            ));
        }
    }
    if !resolved {
        return Err(format!("`{host}` did not resolve to any address"));
    }
    Ok(())
}
