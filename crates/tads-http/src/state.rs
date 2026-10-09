//! Shared application state (repositories + service metadata) injected into every handler.

use std::sync::Arc;

use aws_config::BehaviorVersion;
use sqlx::PgPool;
use tads_domain::Service;
use tads_store::{DeleteRequestRepo, EventRepo, SchemaRepo, TrackRepo, WebhookRepo};

use crate::config::AppConfig;
use crate::error::AppError;

/// Cloneable handle to the store repositories and static service metadata. Repos share one
/// `sqlx::PgPool` (cheap to clone), so `AppState` is `Clone` and shared across requests.
#[derive(Clone)]
pub struct AppState {
    pub schemas: SchemaRepo,
    pub tracks: TrackRepo,
    pub events: EventRepo,
    pub webhooks: WebhookRepo,
    pub delete_requests: DeleteRequestRepo,
    pub service: Arc<Service>,
    /// SQS client + optional poke-queue URL. After a mutation commits, handlers call
    /// [`AppState::poke`] to nudge the webhook relay (replaces the EventBridge schedule).
    sqs: aws_sdk_sqs::Client,
    poke_queue_url: Option<String>,
}

impl AppState {
    /// Build state from configuration and the ambient AWS environment (the Lambda execution
    /// role / region). Called once per cold start: fetches the DB password from Secrets Manager
    /// and opens the Aurora connection pool through RDS Proxy.
    pub async fn from_env() -> Result<Self, AppError> {
        let (pool, config, aws) = connect_pool().await?;
        let sqs = aws_sdk_sqs::Client::new(&aws);
        Ok(Self::from_parts(config, pool, sqs))
    }

    /// Assemble state from an explicit pool — the seam tests use with a testcontainer Postgres.
    pub fn from_parts(config: AppConfig, pool: PgPool, sqs: aws_sdk_sqs::Client) -> Self {
        let service = Service {
            name: config.service.name.clone(),
            description: None,
            service_type: config.service.service_type.clone(),
            api_version: config.service.api_version.clone(),
            service_version: config.service.service_version.clone(),
        };

        Self {
            schemas: SchemaRepo::new(pool.clone()),
            tracks: TrackRepo::new(pool.clone()),
            events: EventRepo::new(pool.clone(), config.max_payload_bytes),
            webhooks: WebhookRepo::new(pool.clone()),
            delete_requests: DeleteRequestRepo::new(
                pool,
                sqs.clone(),
                config.delete_queue_url.clone(),
            ),
            service: Arc::new(service),
            sqs,
            poke_queue_url: config.poke_queue_url.clone(),
        }
    }

    /// Nudge the webhook relay that the outbox has new rows. Best-effort: a failed poke is logged,
    /// not propagated (the relay still catches up on the next poke). No-op if no poke queue is
    /// configured (e.g. local tests).
    pub async fn poke(&self) {
        let Some(url) = &self.poke_queue_url else {
            return;
        };
        if let Err(e) = self
            .sqs
            .send_message()
            .queue_url(url)
            .message_body("{}")
            .send()
            .await
        {
            tracing::warn!(error = %e, "failed to send webhook poke; relay will catch up");
        }
    }
}

/// Axum middleware: after a successful mutating request (`POST`/`PUT`/`DELETE` → 2xx), poke the
/// webhook relay. Centralizes the nudge so individual handlers don't each have to remember it;
/// over-poking is harmless (the relay drains whatever is pending, or no-ops).
pub async fn poke_on_mutation(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mutating = matches!(
        *req.method(),
        http::Method::POST | http::Method::PUT | http::Method::DELETE
    );
    let resp = next.run(req).await;
    if mutating && resp.status().is_success() {
        state.poke().await;
    }
    resp
}

/// Load config, fetch the DB password from Secrets Manager, and open the Aurora pool through RDS
/// Proxy. Shared by `AppState::from_env` and the async workers (which need a pool but not the full
/// `AppState`). Returns the pool, the loaded config, and the resolved AWS config.
pub async fn connect_pool() -> Result<(PgPool, AppConfig, aws_config::SdkConfig), AppError> {
    let config = AppConfig::load()?;
    let aws = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let mut db = config.database_config();
    db.password = fetch_db_password(&aws, &config.database.secret_arn).await?;
    let pool = tads_store::connect(&db)
        .await
        .map_err(|e| AppError::Internal(format!("db connect failed: {e}")))?;
    Ok((pool, config, aws))
}

/// Read the `password` field out of the RDS-generated Secrets Manager secret.
async fn fetch_db_password(
    aws: &aws_config::SdkConfig,
    secret_arn: &str,
) -> Result<String, AppError> {
    let sm = aws_sdk_secretsmanager::Client::new(aws);
    let out = sm
        .get_secret_value()
        .secret_id(secret_arn)
        .send()
        .await
        .map_err(|e| AppError::Internal(format!("get_secret_value failed: {e}")))?;
    let raw = out
        .secret_string()
        .ok_or_else(|| AppError::Internal("db secret has no string value".into()))?;
    let parsed: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| AppError::Internal(format!("db secret is not JSON: {e}")))?;
    parsed
        .get("password")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| AppError::Internal("db secret missing `password`".into()))
}
