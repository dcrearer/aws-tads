//! Delete-worker Lambda (SQS-triggered) — executes async track/event delete requests.
//!
//! Each SQS message body is a delete-request id. The worker advances the request
//! `created → started → done` (or `error`), deleting every Event wholly within
//! `timerange_to_delete` (eternity ⇒ all Events on the track) and, if `delete_track`, removing
//! the Track afterwards. Processing is idempotent: a request already `done` is skipped, so SQS
//! redeliveries are safe.

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;
use tads_domain::{DeleteStatus, ErrorBody, TimeRange};
use tads_http::AppState;

#[tracing::instrument(skip_all, fields(handler = "delete_worker"))]
async fn handle(state: AppState, event: LambdaEvent<Value>) -> Result<(), Error> {
    let records = event
        .payload
        .get("Records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    tracing::info!(records = records.len(), "delete-worker batch received");

    for record in &records {
        let Some(request_id) = record.get("body").and_then(Value::as_str) else {
            tracing::warn!("SQS record missing string body; skipping");
            continue;
        };
        match process_request(&state, request_id).await {
            // The delete wrote outbox rows (events/track removed) — poke the relay to deliver.
            Ok(()) => state.poke().await,
            // Record the failure on the request (best-effort) and log it. We don't propagate the
            // error, so one poisoned request doesn't fail the whole batch / redeliver endlessly.
            Err(e) => {
                tracing::error!(request_id, error = %e, "delete request failed");
                mark_error(&state, request_id, &e.to_string()).await;
            }
        }
    }
    Ok(())
}

async fn process_request(state: &AppState, request_id: &str) -> Result<()> {
    let mut request = state
        .delete_requests
        .get(request_id)
        .await?
        .ok_or_else(|| anyhow!("delete request {request_id} not found"))?;

    // Idempotent: a completed request is a no-op (SQS may redeliver).
    if matches!(request.status, DeleteStatus::Done) {
        return Ok(());
    }

    request.status = DeleteStatus::Started;
    request.updated = Some(Utc::now());
    state.delete_requests.update(&request).await?;

    let track_id = request.track_id.to_string();
    let timerange = request
        .timerange_to_delete
        .unwrap_or_else(TimeRange::eternity);

    let deleted = state
        .events
        .delete_all_within(&track_id, &timerange)
        .await
        .context("deleting events")?;
    tracing::info!(request_id, track_id, deleted, "events deleted");

    if request.delete_track {
        state
            .tracks
            .delete(&track_id)
            .await
            .context("deleting track")?;
        tracing::info!(request_id, track_id, "track deleted");
    }

    request.status = DeleteStatus::Done;
    request.timerange_remaining = None;
    request.updated = Some(Utc::now());
    state.delete_requests.update(&request).await?;
    Ok(())
}

/// Best-effort: stamp `error` status onto the request so clients polling it see the failure.
async fn mark_error(state: &AppState, request_id: &str, summary: &str) {
    match state.delete_requests.get(request_id).await {
        Ok(Some(mut request)) => {
            request.status = DeleteStatus::Error;
            request.updated = Some(Utc::now());
            request.error = Some(ErrorBody {
                kind: "delete_failed".to_string(),
                summary: summary.to_string(),
                traceback: None,
                time: Utc::now(),
            });
            if let Err(e) = state.delete_requests.update(&request).await {
                tracing::error!(request_id, error = %e, "failed to persist error status");
            }
        }
        Ok(None) => tracing::warn!(request_id, "cannot mark error: request not found"),
        Err(e) => tracing::error!(request_id, error = %e, "cannot load request to mark error"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();
    let state = AppState::from_env()
        .await
        .map_err(|e| Error::from(e.to_string()))?;
    lambda_runtime::run(service_fn(move |event| {
        let state = state.clone();
        async move { handle(state, event).await }
    }))
    .await
}
