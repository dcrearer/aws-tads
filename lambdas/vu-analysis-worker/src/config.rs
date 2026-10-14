//! Worker configuration (loaded from `config/vu.yaml` + `VU__*` env overrides).

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    pub tams: TamsConfig,
    pub tads: TadsConfig,
    pub bedrock: BedrockConfig,
    pub window: WindowConfig,
    pub key_moment: KeyMomentConfig,
    /// When true, the worker analyses and logs the derived Events but performs no TADS writes
    /// (no schema/track ensure, no event POST). Useful for validating TAMS auth + inference.
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TamsConfig {
    pub base_url: String,
    /// Inline OAuth2 client-credentials for the TAMS control plane. When absent, TAMS calls are
    /// made unauthenticated. The secret is read straight from config (not Secrets Manager).
    #[serde(default)]
    pub oauth: Option<InlineOAuthConfig>,
}

/// OAuth2 client-credentials supplied directly in config (client_secret inline, no Secrets Manager).
#[derive(Debug, Clone, Deserialize)]
pub struct InlineOAuthConfig {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
    #[serde(default)]
    pub scope: Option<String>,
}

impl InlineOAuthConfig {
    /// Scope, treating an empty string as absent.
    pub fn scope(&self) -> Option<String> {
        self.scope
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TadsConfig {
    pub base_url: String,
    /// Inline OAuth2 client-credentials for the TADS writeback API. When absent, writeback calls are
    /// unauthenticated (and a real TADS will reject them). Supplied straight from config, same as
    /// `tams.oauth` — no Secrets Manager.
    #[serde(default)]
    pub oauth: Option<InlineOAuthConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BedrockConfig {
    pub model_id: String,
    pub max_output_tokens: i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WindowConfig {
    pub lookback_n: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KeyMomentConfig {
    pub schema_external_id: String,
}

impl WorkerConfig {
    /// Load from `VU_CONFIG` (default `config/vu.yaml`) layered under `VU__*` env overrides.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> serde_json::Value {
        serde_json::json!({
            "tams": { "base_url": "https://tams" },
            "tads": {
                "base_url": "https://tads",
                "oauth": { "token_url": "t", "client_id": "c", "client_secret": "s" }
            },
            "bedrock": { "model_id": "m", "max_output_tokens": 4096 },
            "window": { "lookback_n": 3 },
            "key_moment": { "schema_external_id": "vu:key-moment:v1" }
        })
    }

    #[test]
    fn tams_oauth_is_optional() {
        let cfg: WorkerConfig = serde_json::from_value(base()).unwrap();
        assert!(cfg.tams.oauth.is_none());
    }

    #[test]
    fn parses_inline_tams_oauth() {
        let mut v = base();
        v["tams"]["oauth"] = serde_json::json!({
            "token_url": "https://idp/token",
            "client_id": "cid",
            "client_secret": "shh"
        });
        let cfg: WorkerConfig = serde_json::from_value(v).unwrap();
        let o = cfg.tams.oauth.expect("inline oauth present");
        assert_eq!(o.client_secret, "shh");
        assert_eq!(o.scope(), None); // absent -> None
    }
}
