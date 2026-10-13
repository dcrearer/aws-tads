//! Webhook receiver Lambda (API Gateway HTTP API → Lambda).
//!
//! Verifies the inbound TAMS shared secret, then: `flows/created` ensures the TADS Track(s) for the
//! Source (Phase A); `flows/segments_added` is enqueued onto the SQS work queue for the analyzer
//! (Phase B). Runs outside the VPC (TADS/SQS reached over public HTTPS / SDK).

use std::sync::Arc;

use aws_config::BehaviorVersion;
use lambda_http::{Body, Error, Request, Response, run, service_fn};
use serde_json::Value;
use tads_lambda_webhook_receiver::{ENABLED_TYPES, Incoming, build_track, parse_incoming};
use vu_auth::{ClientCredentials, TokenProvider};
use vu_config::InferenceConfig;
use vu_tads::TadsClient;
use vu_tams::verify_api_key;

struct State {
    tads: TadsClient,
    sqs: aws_sdk_sqs::Client,
    queue_url: String,
    /// Expected inbound shared secret; `None` disables verification (dev only).
    expected_key: Option<String>,
    header: String,
    entity_base: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();

    let cfg = InferenceConfig::load().map_err(|e| Error::from(e.to_string()))?;
    let aws = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let http = reqwest::Client::builder()
        .build()
        .map_err(|e| Error::from(e.to_string()))?;

    let tads_creds = ClientCredentials::from_secret(
        &aws,
        &cfg.tads_secret_arn,
        &cfg.tads_token_url,
        &cfg.tads_scope,
    )
    .await?;
    let tads = TadsClient::new(
        http.clone(),
        &cfg.tads_base_url,
        TokenProvider::new(http.clone(), tads_creds),
    );

    let expected_key = if cfg.webhook_secret_arn.is_empty() {
        tracing::warn!("no webhook_secret_arn configured — inbound verification is DISABLED");
        None
    } else {
        Some(fetch_secret_string(&aws, &cfg.webhook_secret_arn).await?)
    };

    let state = Arc::new(State {
        tads,
        sqs: aws_sdk_sqs::Client::new(&aws),
        queue_url: cfg.work_queue_url,
        expected_key,
        header: cfg.webhook_header,
        entity_base: cfg.tams_entity_base,
    });

    run(service_fn(move |req: Request| {
        let state = state.clone();
        async move { Ok::<_, Error>(handle(&state, req).await) }
    }))
    .await
}

#[tracing::instrument(skip_all, fields(handler = "webhook_receiver"))]
async fn handle(state: &State, req: Request) -> Response<Body> {
    // 1. Verify the inbound shared secret.
    if let Some(expected) = &state.expected_key {
        let provided = req
            .headers()
            .get(&state.header)
            .and_then(|v| v.to_str().ok());
        if !verify_api_key(expected, provided) {
            return resp(401, "unauthorized");
        }
    }

    // 2. Parse the callback body.
    let body: Value = match serde_json::from_slice(req.body().as_ref()) {
        Ok(v) => v,
        Err(e) => return resp(400, &format!("invalid JSON body: {e}")),
    };

    // 3. Dispatch.
    match parse_incoming(&body) {
        Incoming::FlowCreated { source_id } => match ensure_tracks(state, &source_id).await {
            Ok(n) => resp(200, &format!("ensured {n} track(s) for source {source_id}")),
            Err(e) => {
                tracing::warn!(error = %e, %source_id, "track ensure failed");
                resp(502, "track ensure failed")
            }
        },
        Incoming::SegmentsAdded { event } => match enqueue(state, &event).await {
            Ok(()) => resp(202, "enqueued"),
            Err(e) => {
                tracing::warn!(error = %e, "enqueue failed");
                resp(502, "enqueue failed")
            }
        },
        Incoming::Ignored(kind) => {
            tracing::debug!(%kind, "ignored callback");
            resp(200, "ignored")
        }
    }
}

/// Phase A — PUT one Track per enabled analysis type (idempotent).
async fn ensure_tracks(state: &State, source_id: &str) -> Result<usize, String> {
    for ty in ENABLED_TYPES {
        let schema_id = state
            .tads
            .resolve_schema(ty.external_id())
            .await
            .map_err(|e| e.to_string())?;
        let track = build_track(source_id, *ty, schema_id, &state.entity_base);
        state
            .tads
            .put_track(&track)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(ENABLED_TYPES.len())
}

/// Phase B — enqueue the raw `segments_added` event for the analyzer to consume.
async fn enqueue(state: &State, event: &Value) -> Result<(), String> {
    state
        .sqs
        .send_message()
        .queue_url(&state.queue_url)
        .message_body(event.to_string())
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn fetch_secret_string(aws: &aws_config::SdkConfig, arn: &str) -> Result<String, Error> {
    let sm = aws_sdk_secretsmanager::Client::new(aws);
    let out = sm.get_secret_value().secret_id(arn).send().await?;
    let raw = out
        .secret_string()
        .ok_or_else(|| Error::from("webhook secret has no string value"))?;
    // Accept either a plain string or {"api_key": "..."}.
    if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(raw)
        && let Some(k) = map.get("api_key").and_then(Value::as_str)
    {
        return Ok(k.to_string());
    }
    Ok(raw.to_string())
}

fn resp(status: u16, msg: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(Body::from(msg.to_string()))
        .expect("valid response")
}
