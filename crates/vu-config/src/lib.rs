//! Shared configuration for the video-understanding Lambdas (`analyzer`, `webhook-receiver`).
//!
//! One `inference:` section in `config/default.yaml`, overridable at runtime by
//! `TADS__INFERENCE__<KEY>` env vars (set per-environment by CDK). Secrets (OAuth client
//! credentials, the inbound webhook key) are referenced by ARN and fetched from Secrets Manager at
//! cold start — never stored here. Each Lambda reads the subset of fields it needs; unused fields
//! stay empty.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct InferenceConfig {
    // --- TADS (both Lambdas write to TADS) ---
    pub tads_base_url: String,
    pub tads_token_url: String,
    pub tads_secret_arn: String,
    #[serde(default = "default_tads_scope")]
    pub tads_scope: String,

    // --- TAMS (analyzer fetches media) ---
    #[serde(default)]
    pub tams_token_url: String,
    #[serde(default)]
    pub tams_secret_arn: String,
    #[serde(default)]
    pub tams_scope: String,

    // --- analyzer ---
    #[serde(default)]
    pub bedrock_model_id: String,
    #[serde(default = "default_fps")]
    pub frames_per_second: f32,

    // --- webhook-receiver ---
    /// SQS work queue the receiver enqueues `segments_added` jobs onto.
    #[serde(default)]
    pub work_queue_url: String,
    /// Base for a Track's `timeline_reference.entity_id`, e.g. `tams://tams.example.com`
    /// (the receiver appends `/sources/{source_id}`).
    #[serde(default)]
    pub tams_entity_base: String,
    /// Secrets Manager ARN of the inbound webhook shared secret.
    #[serde(default)]
    pub webhook_secret_arn: String,
    /// Header the TAMS callback carries its shared secret in.
    #[serde(default = "default_webhook_header")]
    pub webhook_header: String,
}

fn default_tads_scope() -> String {
    "tads-api/write".to_string()
}
fn default_fps() -> f32 {
    1.0
}
fn default_webhook_header() -> String {
    "x-api-key".to_string()
}

impl InferenceConfig {
    /// Load the `inference` section (YAML file layered under `TADS__` env overrides).
    pub fn load() -> Result<Self, config::ConfigError> {
        let path = std::env::var("TADS_CONFIG").unwrap_or_else(|_| "config/default.yaml".into());
        config::Config::builder()
            .add_source(config::File::with_name(&path).required(false))
            .add_source(
                config::Environment::with_prefix("TADS")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?
            .get::<InferenceConfig>("inference")
    }
}
