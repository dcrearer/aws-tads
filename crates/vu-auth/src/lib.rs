//! Shared OAuth2 **client-credentials** token provider for the outbound TADS and TAMS clients.
//!
//! Both services authenticate the same way (a machine-to-machine client id/secret exchanged for a
//! bearer token), so this is one implementation parameterized per service rather than two copies.
//! Credentials come from Secrets Manager at cold start; the token is cached and refreshed shortly
//! before it expires. See docs/inference-plan.md §7.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::sync::Mutex;

/// Refresh this many seconds before the token's stated expiry, so an in-flight request never races
/// expiry.
const EXPIRY_SKEW: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("token endpoint request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("secrets manager get_secret_value failed: {0}")]
    Secret(String),
    #[error("client-credentials secret is not the expected JSON shape: {0}")]
    SecretShape(String),
    #[error("token endpoint returned {status}: {body}")]
    TokenStatus { status: u16, body: String },
}

/// OAuth2 client-credentials for one service.
#[derive(Debug, Clone)]
pub struct ClientCredentials {
    pub client_id: String,
    pub client_secret: String,
    /// The token endpoint (`.../oauth2/token`).
    pub token_url: String,
    /// Space-delimited scope(s), e.g. `tads-api/write`.
    pub scope: String,
}

impl ClientCredentials {
    /// Load credentials from a Secrets Manager secret whose JSON holds `client_id` and
    /// `client_secret`. `token_url` and `scope` are non-secret config.
    pub async fn from_secret(
        aws: &aws_config::SdkConfig,
        secret_arn: &str,
        token_url: impl Into<String>,
        scope: impl Into<String>,
    ) -> Result<Self, AuthError> {
        let sm = aws_sdk_secretsmanager::Client::new(aws);
        let out = sm
            .get_secret_value()
            .secret_id(secret_arn)
            .send()
            .await
            .map_err(|e| AuthError::Secret(e.to_string()))?;
        let raw = out
            .secret_string()
            .ok_or_else(|| AuthError::SecretShape("secret has no string value".into()))?;
        #[derive(Deserialize)]
        struct Secret {
            client_id: String,
            client_secret: String,
        }
        let s: Secret = serde_json::from_str(raw)
            .map_err(|e| AuthError::SecretShape(format!("not JSON with client_id/secret: {e}")))?;
        Ok(Self {
            client_id: s.client_id,
            client_secret: s.client_secret,
            token_url: token_url.into(),
            scope: scope.into(),
        })
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    /// Lifetime in seconds. Absent → treated as already-expired (always refresh).
    #[serde(default)]
    expires_in: u64,
}

#[derive(Clone)]
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

/// A cloneable, shared provider that hands out a valid bearer token, refreshing as needed.
#[derive(Clone)]
pub struct TokenProvider {
    http: reqwest::Client,
    creds: ClientCredentials,
    cache: Arc<Mutex<Option<CachedToken>>>,
}

impl TokenProvider {
    pub fn new(http: reqwest::Client, creds: ClientCredentials) -> Self {
        Self {
            http,
            creds,
            cache: Arc::new(Mutex::new(None)),
        }
    }

    /// Return a valid bearer token value (without the `Bearer ` prefix), refreshing if the cached
    /// one is missing or within [`EXPIRY_SKEW`] of expiry.
    pub async fn token(&self) -> Result<String, AuthError> {
        let mut cache = self.cache.lock().await;
        if let Some(c) = cache.as_ref()
            && Instant::now() < c.expires_at
        {
            return Ok(c.access_token.clone());
        }
        let fetched = self.fetch().await?;
        let expires_at = Instant::now() + effective_ttl(fetched.expires_in);
        *cache = Some(CachedToken {
            access_token: fetched.access_token.clone(),
            expires_at,
        });
        Ok(fetched.access_token)
    }

    #[tracing::instrument(skip_all, fields(token_url = %self.creds.token_url))]
    async fn fetch(&self) -> Result<TokenResponse, AuthError> {
        let resp = self
            .http
            .post(&self.creds.token_url)
            .basic_auth(&self.creds.client_id, Some(&self.creds.client_secret))
            .form(&[
                ("grant_type", "client_credentials"),
                ("scope", self.creds.scope.as_str()),
            ])
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AuthError::TokenStatus {
                status: status.as_u16(),
                body,
            });
        }
        Ok(resp.json::<TokenResponse>().await?)
    }
}

/// How long a freshly-fetched token should be trusted: its stated lifetime minus the skew, and
/// never zero/negative (a short or absent lifetime just means refresh next call).
fn effective_ttl(expires_in_secs: u64) -> Duration {
    Duration::from_secs(expires_in_secs).saturating_sub(EXPIRY_SKEW)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_subtracts_skew() {
        assert_eq!(effective_ttl(3600), Duration::from_secs(3570));
    }

    #[test]
    fn short_or_absent_lifetime_never_goes_negative() {
        // expires_in <= skew → zero TTL (refresh on next call), not an underflow.
        assert_eq!(effective_ttl(30), Duration::ZERO);
        assert_eq!(effective_ttl(0), Duration::ZERO);
        assert_eq!(effective_ttl(10), Duration::ZERO);
    }
}
