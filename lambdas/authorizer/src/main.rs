//! Authorizer binary — HTTP API v2 REQUEST authorizer with simple (`isAuthorized`) responses.

use std::collections::HashMap;
use std::sync::Arc;

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde::Deserialize;
use serde_json::{Value, json};
use tads_lambda_authorizer::{Config, bearer_token, verify};

/// Subset of the HTTP API v2 authorizer event we need. `identitySource` carries the configured
/// `$request.header.Authorization`; we fall back to the raw header map if it is absent.
#[derive(Debug, Default, Deserialize)]
struct AuthEvent {
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default, rename = "identitySource")]
    identity_source: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();

    let issuer =
        std::env::var("TADS_JWT_ISSUER").map_err(|_| Error::from("TADS_JWT_ISSUER unset"))?;
    let client_id = std::env::var("TADS_CLIENT_ID")
        .ok()
        .filter(|s| !s.is_empty());
    let cfg = Arc::new(Config { issuer, client_id });

    lambda_runtime::run(service_fn(move |event: LambdaEvent<Value>| {
        let cfg = cfg.clone();
        async move {
            let ev: AuthEvent = serde_json::from_value(event.payload).unwrap_or_default();
            let raw = ev
                .identity_source
                .into_iter()
                .next()
                .or_else(|| ev.headers.get("authorization").cloned())
                .unwrap_or_default();

            let authorized = match bearer_token(&raw) {
                Some(token) => match verify(token, &cfg).await {
                    Ok(claims) => {
                        tracing::info!(client_id = %claims.client_id, sub = %claims.sub, "authorized");
                        true
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "denied");
                        false
                    }
                },
                None => {
                    tracing::warn!("no bearer token on request");
                    false
                }
            };

            Ok::<Value, Error>(json!({ "isAuthorized": authorized }))
        }
    }))
    .await
}
