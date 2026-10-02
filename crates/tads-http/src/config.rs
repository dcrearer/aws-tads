//! Application configuration (YAML + env overrides, via the `config` crate).
//!
//! Base file: `config/default.yaml` (or `$TADS_CONFIG`). Every value is overridable by an env
//! var `TADS__<SECTION>__<KEY>` (double-underscore nesting), which is how the Lambdas receive
//! their table/queue/bucket names at runtime.

use serde::Deserialize;
use tads_store::StoreConfig;

use crate::error::AppError;

fn default_max_payload_bytes() -> usize {
    StoreConfig::DEFAULT_MAX_PAYLOAD_BYTES
}

#[derive(Debug, Clone, Deserialize)]
pub struct Tables {
    pub schemas: String,
    pub tracks: String,
    pub events: String,
    pub webhooks: String,
    pub delete_requests: String,
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
    pub tables: Tables,
    pub delete_queue_url: String,
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

    pub fn store_config(&self) -> StoreConfig {
        StoreConfig {
            schemas_table: self.tables.schemas.clone(),
            tracks_table: self.tables.tracks.clone(),
            events_table: self.tables.events.clone(),
            webhooks_table: self.tables.webhooks.clone(),
            delete_requests_table: self.tables.delete_requests.clone(),
            delete_queue_url: self.delete_queue_url.clone(),
            max_payload_bytes: self.max_payload_bytes,
        }
    }
}
