//! Webhook-dispatcher binary — EventBridge-scheduled; drains the transactional outbox and fans
//! each change out to matching webhook subscriptions.

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;
use tads_http::connect_pool;
use tads_lambda_webhook_dispatcher::Dispatcher;
use tads_store::{OutboxRepo, WebhookRepo};

const BATCH: i64 = 100;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();

    let (pool, _config, _aws) = connect_pool()
        .await
        .map_err(|e| Error::from(e.to_string()))?;
    let outbox = OutboxRepo::new(pool.clone());
    let repo = WebhookRepo::new(pool);
    let http = reqwest::Client::builder()
        .build()
        .map_err(|e| Error::from(e.to_string()))?;
    let dispatcher = Dispatcher::new(repo, http);

    lambda_runtime::run(service_fn(move |_event: LambdaEvent<Value>| {
        let outbox = outbox.clone();
        let dispatcher = dispatcher.clone();
        async move {
            // Drain the outbox in batches until empty (one scheduled invocation).
            loop {
                let rows = outbox
                    .claim_batch(BATCH)
                    .await
                    .map_err(|e| Error::from(e.to_string()))?;
                if rows.is_empty() {
                    break;
                }
                tracing::info!(rows = rows.len(), "dispatching outbox batch");
                if let Err(e) = dispatcher.dispatch(&rows).await {
                    tracing::error!(error = %e, "dispatch failed");
                }
            }
            Ok::<(), Error>(())
        }
    }))
    .await
}
