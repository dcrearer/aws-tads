//! Application configuration (YAML + env overrides, via the `config` crate).
//!
//! Base file: `config/default.yaml` (or `$TADS_CONFIG`). Every value is overridable by an env
//! var `TADS__<SECTION>__<KEY>` (double-underscore nesting), which is how the Lambdas receive
//! their database/queue settings at runtime.

use serde::Deserialize;
use tads_store::{DatabaseConfig, StoreConfig};

use crate::error::AppError;

fn default_max_payload_bytes() -> usize {
    StoreConfig::DEFAULT_MAX_PAYLOAD_BYTES
}

fn default_pg_port() -> u16 {
    5432
}

fn default_pg_user() -> String {
    "tads".to_string()
}

fn default_max_connections() -> u32 {
    5
}

/// Aurora/RDS-Proxy connection settings. The password is not configured here — it is fetched from
/// Secrets Manager (`secret_arn`) at cold start.
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseSettings {
    /// RDS Proxy endpoint host.
    pub proxy_endpoint: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    pub db_name: String,
    #[serde(default = "default_pg_user")]
    pub username: String,
    /// Secrets Manager ARN of the RDS-generated credentials secret.
    pub secret_arn: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceInfo {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub service_type: String,
    pub api_version: String,
    pub service_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub database: DatabaseSettings,
    pub delete_queue_url: String,
    /// Webhook "poke" queue — mutation handlers nudge it after commit so the relay drains the
    /// outbox promptly (replaces the EventBridge schedule). Absent ⇒ poke is skipped.
    #[serde(default)]
    pub poke_queue_url: Option<String>,
    /// Webhook delivery queue — the relay fans out one message per (change × subscriber). Required
    /// by the relay; unused elsewhere.
    #[serde(default)]
    pub delivery_queue_url: Option<String>,
    #[serde(default = "default_max_payload_bytes")]
    pub max_payload_bytes: usize,
    pub service: ServiceInfo,
}

impl AppConfig {
    /// Load config, layering the YAML file (optional) under env overrides.
    pub fn load() -> Result<Self, AppError> {
        let path = std::env::var("TADS_CONFIG").unwrap_or_else(|_| "config/default.yaml".into());
        let cfg = config::Config::builder()
            .add_source(config::File::with_name(&path).required(false))
            .add_source(
                config::Environment::with_prefix("TADS")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .map_err(|e| AppError::Internal(format!("config build failed: {e}")))?;
        cfg.try_deserialize()
            .map_err(|e| AppError::Internal(format!("config invalid: {e}")))
    }

    /// The store's database settings (password left empty — filled from Secrets Manager by
    /// [`crate::state::AppState::from_env`]).
    pub fn database_config(&self) -> DatabaseConfig {
        DatabaseConfig {
            proxy_endpoint: self.database.proxy_endpoint.clone(),
            port: self.database.port,
            db_name: self.database.db_name.clone(),
            username: self.database.username.clone(),
            region: String::new(),
            password: String::new(),
            max_connections: self.database.max_connections,
        }
    }
}
