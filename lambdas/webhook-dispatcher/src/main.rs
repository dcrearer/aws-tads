//! Webhook-dispatcher binary — wires config/state to the [`Dispatcher`] and consumes the stream.

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;
use tads_http::{AppConfig, AppState};
use tads_lambda_webhook_dispatcher::{Dispatcher, TableRoles};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();

    let config = AppConfig::load().map_err(|e| Error::from(e.to_string()))?;
    let state = AppState::from_env()
        .await
        .map_err(|e| Error::from(e.to_string()))?;
    let roles = TableRoles {
        schemas: config.tables.schemas.clone(),
        tracks: config.tables.tracks.clone(),
        events: config.tables.events.clone(),
    };
    let http = reqwest::Client::builder()
        .build()
        .map_err(|e| Error::from(e.to_string()))?;
    let dispatcher = Dispatcher::new(state.webhooks.clone(), roles, http);

    lambda_runtime::run(service_fn(move |event: LambdaEvent<Value>| {
        let dispatcher = dispatcher.clone();
        async move {
            let records = event
                .payload
                .get("Records")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            tracing::info!(records = records.len(), "webhook-dispatcher batch received");
            if let Err(e) = dispatcher.dispatch(&records).await {
                tracing::error!(error = %e, "dispatch failed");
            }
            Ok::<(), Error>(())
        }
    }))
    .await
}
