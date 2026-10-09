//! Webhook relay (in-VPC, poke-triggered).
//!
//! Drains the transactional outbox and fans each change out to **one delivery-queue message per
//! matching subscriber**, marking rows dispatched **only after** the sends succeed — all within one
//! transaction that holds the row locks. A crash or send failure rolls the transaction back, so the
//! rows are re-claimed on the next poke (at-least-once). The SQS poke that triggers this is a pure
//! signal; its body is ignored.

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;
use tads_http::connect_pool;
use tads_lambda_webhook::{DeliveryJob, build_jobs, from_outbox};
use tads_store::{OutboxRepo, WebhookRepo};

/// Outbox rows claimed per transaction.
const BATCH: i64 = 100;
/// SQS SendMessageBatch hard limit.
const SQS_BATCH: usize = 10;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();
    let (pool, config, aws) = connect_pool()
        .await
        .map_err(|e| Error::from(e.to_string()))?;
    let delivery_queue_url = config
        .delivery_queue_url
        .clone()
        .ok_or_else(|| Error::from("TADS__DELIVERY_QUEUE_URL is not set"))?;
    let outbox = OutboxRepo::new(pool.clone());
    let webhooks = WebhookRepo::new(pool);
    let sqs = aws_sdk_sqs::Client::new(&aws);

    lambda_runtime::run(service_fn(move |_event: LambdaEvent<Value>| {
        let outbox = outbox.clone();
        let webhooks = webhooks.clone();
        let sqs = sqs.clone();
        let dq = delivery_queue_url.clone();
        async move { drain(&outbox, &webhooks, &sqs, &dq).await }
    }))
    .await
}

#[tracing::instrument(skip_all, fields(handler = "webhook_relay"))]
async fn drain(
    outbox: &OutboxRepo,
    webhooks: &WebhookRepo,
    sqs: &aws_sdk_sqs::Client,
    delivery_queue_url: &str,
) -> Result<(), Error> {
    loop {
        let (mut tx, rows) = outbox
            .claim_tx(BATCH)
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        if rows.is_empty() {
            break;
        }

        // Load subscriptions once per batch and fan out.
        let subs = webhooks
            .list_all()
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        let mut jobs: Vec<DeliveryJob> = Vec::new();
        if !subs.is_empty() {
            for row in &rows {
                if let Some(change) = from_outbox(row) {
                    jobs.extend(build_jobs(&change, &row.id.to_string(), &subs));
                }
            }
        }

        // Enqueue BEFORE marking. If this fails we return (tx drops → rollback → re-claimed).
        send_all(sqs, delivery_queue_url, &jobs).await?;

        let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
        OutboxRepo::mark_dispatched(&mut tx, &ids)
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        tx.commit().await.map_err(|e| Error::from(e.to_string()))?;
        tracing::info!(rows = rows.len(), jobs = jobs.len(), "relayed outbox batch");
    }
    Ok(())
}

async fn send_all(
    sqs: &aws_sdk_sqs::Client,
    delivery_queue_url: &str,
    jobs: &[DeliveryJob],
) -> Result<(), Error> {
    use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
    for chunk in jobs.chunks(SQS_BATCH) {
        let mut req = sqs.send_message_batch().queue_url(delivery_queue_url);
        for (i, job) in chunk.iter().enumerate() {
            let body = serde_json::to_string(job).map_err(|e| Error::from(e.to_string()))?;
            let entry = SendMessageBatchRequestEntry::builder()
                .id(i.to_string())
                .message_body(body)
                .build()
                .map_err(|e| Error::from(e.to_string()))?;
            req = req.entries(entry);
        }
        let out = req.send().await.map_err(|e| Error::from(e.to_string()))?;
        if !out.failed().is_empty() {
            return Err(Error::from(format!(
                "sqs send_message_batch had {} failed entries",
                out.failed().len()
            )));
        }
    }
    Ok(())
}
