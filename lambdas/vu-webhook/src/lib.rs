//! TAMS `flows/segments_added` webhook receiver.
//!
//! Verifies the inbound HMAC signature in constant time, then enqueues the raw payload to the
//! analysis SQS queue and returns `202` — no inference inline (poke→SQS→worker). The route sits
//! **outside** the Cognito authorizer (TAMS presents an HMAC, not a bearer token), so the signature
//! check is the only gate. The router is exposed as a library so it can be driven in tests.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use serde::Deserialize;
use vu_tams::{SegmentsAddedEvent, verify_signature};

/// Default header the TAMS signature is presented in.
fn default_signature_header() -> String {
    "x-tams-signature".to_string()
}

/// Receiver configuration (loaded from `config/vu.yaml` + `VU__*` env).
#[derive(Debug, Clone, Deserialize)]
pub struct WebhookConfig {
    pub analysis_queue_url: String,
    pub tams: TamsSignatureConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TamsSignatureConfig {
    pub signature_secret_arn: String,
    #[serde(default = "default_signature_header")]
    pub signature_header: String,
}

impl WebhookConfig {
    /// Load config from `VU_CONFIG` (default `config/vu.yaml`) layered under `VU__*` env overrides.
    pub fn load() -> Result<Self, config::ConfigError> {
        let path = std::env::var("VU_CONFIG").unwrap_or_else(|_| "config/vu.yaml".to_string());
        config::Config::builder()
            .add_source(config::File::with_name(&path).required(false))
            .add_source(
                config::Environment::with_prefix("VU")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?
            .try_deserialize()
    }
}

/// Runtime state: the SQS client + queue, the HMAC key, and the signature header name.
#[derive(Clone)]
pub struct WebhookState {
    sqs: aws_sdk_sqs::Client,
    queue_url: String,
    signing_key: Arc<Vec<u8>>,
    signature_header: String,
}

impl WebhookState {
    pub fn new(
        sqs: aws_sdk_sqs::Client,
        queue_url: impl Into<String>,
        signing_key: Vec<u8>,
        signature_header: impl Into<String>,
    ) -> Self {
        Self {
            sqs,
            queue_url: queue_url.into(),
            signing_key: Arc::new(signing_key),
            signature_header: signature_header.into(),
        }
    }

    /// Build state from config + AWS environment: load the signing key from Secrets Manager.
    pub async fn from_env() -> Result<Self, String> {
        let cfg = WebhookConfig::load().map_err(|e| format!("config load failed: {e}"))?;
        let aws = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let sqs = aws_sdk_sqs::Client::new(&aws);

        let sm = aws_sdk_secretsmanager::Client::new(&aws);
        let out = sm
            .get_secret_value()
            .secret_id(&cfg.tams.signature_secret_arn)
            .send()
            .await
            .map_err(|e| format!("get_secret_value failed: {e}"))?;
        let raw = out
            .secret_string()
            .ok_or("signature secret has no string value")?;

        Ok(Self::new(
            sqs,
            cfg.analysis_queue_url,
            signing_key_from_secret(raw),
            cfg.tams.signature_header,
        ))
    }
}

/// Interpret the stored secret as the HMAC key: a JSON `{"signing_key"|"key": ...}`, else raw bytes.
fn signing_key_from_secret(raw: &str) -> Vec<u8> {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(raw)
        && let Some(k) = v
            .get("signing_key")
            .or_else(|| v.get("key"))
            .and_then(|x| x.as_str())
    {
        return k.as_bytes().to_vec();
    }
    raw.as_bytes().to_vec()
}

/// Build the receiver router.
pub fn router(state: WebhookState) -> Router {
    Router::new()
        .route("/webhooks/tams/segments-added", post(receive))
        .with_state(state)
}

#[tracing::instrument(skip_all, fields(handler = "vu_webhook"))]
async fn receive(State(state): State<WebhookState>, headers: HeaderMap, body: Bytes) -> Response {
    // 1. Signature header must be present and valid (constant-time HMAC over the raw body).
    let Some(signature) = headers
        .get(state.signature_header.as_str())
        .and_then(|v| v.to_str().ok())
    else {
        tracing::warn!("missing or non-ascii signature header");
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if !verify_signature(&state.signing_key, &body, signature) {
        tracing::warn!("invalid TAMS webhook signature");
        return StatusCode::UNAUTHORIZED.into_response();
    }

    // 2. Validate the payload is a well-formed segments_added event before enqueuing.
    match serde_json::from_slice::<SegmentsAddedEvent>(&body) {
        Ok(event) => {
            tracing::info!(
                flow_id = %event.flow_id,
                segments = event.segments.len(),
                "accepted segments_added webhook"
            );
        }
        Err(e) => {
            tracing::warn!(error = %e, "malformed segments_added payload");
            return StatusCode::BAD_REQUEST.into_response();
        }
    }

    // 3. Enqueue the raw payload for the worker. Do not do inference inline.
    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s.to_string(),
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    match state
        .sqs
        .send_message()
        .queue_url(&state.queue_url)
        .message_body(body_str)
        .send()
        .await
    {
        Ok(_) => StatusCode::ACCEPTED.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to enqueue analysis job");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signing_key_reads_json_or_raw() {
        assert_eq!(signing_key_from_secret(r#"{"signing_key":"abc"}"#), b"abc");
        assert_eq!(signing_key_from_secret(r#"{"key":"xyz"}"#), b"xyz");
        assert_eq!(signing_key_from_secret("raw-secret"), b"raw-secret");
    }
}
