//! Webhook delivery DLQ handler (in-VPC, SQS-triggered).
//!
//! A [`DeliveryJob`] that exhausted its delivery retries lands on the delivery DLQ. We mark that
//! webhook `error` (preserving its api key) so it stops matching future changes until an operator
//! re-enables it. Other subscribers are unaffected — failure is isolated per subscriber.

use chrono::Utc;
use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;
use tads_domain::ErrorBody;
use tads_http::connect_pool;
use tads_lambda_webhook::DeliveryJob;
use tads_store::WebhookRepo;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();
    let (pool, _config, _aws) = connect_pool()
        .await
        .map_err(|e| Error::from(e.to_string()))?;
    let webhooks = WebhookRepo::new(pool);
    lambda_runtime::run(service_fn(move |event: LambdaEvent<Value>| {
        let webhooks = webhooks.clone();
        async move { handle(&webhooks, event).await }
    }))
    .await
}

#[tracing::instrument(skip_all, fields(handler = "webhook_dlq"))]
async fn handle(webhooks: &WebhookRepo, event: LambdaEvent<Value>) -> Result<(), Error> {
    let records = event
        .payload
        .get("Records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for record in &records {
        let Some(body) = record.get("body").and_then(Value::as_str) else {
            tracing::warn!("DLQ record missing string body; skipping");
            continue;
        };
        let Ok(job) = serde_json::from_str::<DeliveryJob>(body) else {
            tracing::warn!("DLQ record is not a DeliveryJob; skipping");
            continue;
        };
        let err = ErrorBody {
            kind: "delivery_failed".into(),
            summary: format!("delivery to {} exhausted retries", job.url),
            traceback: None,
            time: Utc::now(),
        };
        match webhooks.mark_error(&job.webhook_id, &err).await {
            Ok(true) => {
                tracing::warn!(webhook_id = %job.webhook_id, "marked webhook error (delivery DLQ)")
            }
            Ok(false) => {
                tracing::warn!(webhook_id = %job.webhook_id, "webhook not found for DLQ error mark")
            }
            Err(e) => {
                tracing::error!(webhook_id = %job.webhook_id, error = %e, "failed to mark webhook error")
            }
        }
    }
    Ok(())
}
