//! OAuth2 **client-credentials** access-token acquisition for the TAMS/TADS clients, plus loading
//! the client secret from AWS Secrets Manager.
//!
//! A [`TokenProvider`] fetches a bearer token from the IdP token endpoint and caches it until just
//! before it expires, so the hot path makes no network call per request. The HTTP fetch and the
//! clock are both injectable traits ([`TokenFetcher`], [`Clock`]) so the caching/expiry logic is
//! unit-tested without a live IdP.

pub mod error;

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use tokio::sync::Mutex;

pub use error::VuAuthError;

/// Default clock skew: refresh a token this long before its stated expiry.
const DEFAULT_SKEW_SECS: i64 = 60;
/// Fallback lifetime when the token endpoint omits `expires_in`.
const DEFAULT_LIFETIME_SECS: i64 = 3600;

/// OAuth2 client-credentials configuration. The secret is loaded from Secrets Manager; the token
/// URL and scope come from application config.
#[derive(Debug, Clone)]
pub struct ClientCredentials {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
    pub scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SecretShape {
    client_id: String,
    client_secret: String,
}

impl ClientCredentials {
    /// Build credentials from values supplied directly (e.g. inline application config) rather than
    /// from Secrets Manager.
    pub fn from_values(
        token_url: String,
        client_id: String,
        client_secret: String,
        scope: Option<String>,
    ) -> Self {
        Self {
            token_url,
            client_id,
            client_secret,
            scope,
        }
    }

    /// Load `client_id`/`client_secret` from a JSON Secrets Manager secret and combine with the
    /// configured token URL and optional scope.
    pub async fn from_secret(
        sm: &aws_sdk_secretsmanager::Client,
        secret_arn: &str,
        token_url: String,
        scope: Option<String>,
    ) -> Result<Self, VuAuthError> {
        let out = sm
            .get_secret_value()
            .secret_id(secret_arn)
            .send()
            .await
            .map_err(|e| VuAuthError::SecretsManager(e.to_string()))?;
        let raw = out
            .secret_string()
            .ok_or_else(|| VuAuthError::SecretParse("secret has no string value".into()))?;
        let parsed: SecretShape =
            serde_json::from_str(raw).map_err(|e| VuAuthError::SecretParse(e.to_string()))?;
        Ok(Self {
            token_url,
            client_id: parsed.client_id,
            client_secret: parsed.client_secret,
            scope,
        })
    }
}

/// A freshly fetched token and its lifetime.
#[derive(Debug, Clone)]
pub struct FetchedToken {
    pub access_token: String,
    pub lifetime: Duration,
}

/// Abstraction over the token-endpoint HTTP call so caching can be tested offline.
pub trait TokenFetcher: Send + Sync {
    fn fetch(
        &self,
        creds: &ClientCredentials,
    ) -> impl std::future::Future<Output = Result<FetchedToken, VuAuthError>> + Send;
}

/// Injectable clock.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// Wall-clock implementation.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<i64>,
}

/// The real token endpoint fetcher (reqwest, `application/x-www-form-urlencoded`).
#[derive(Debug, Clone)]
pub struct HttpTokenFetcher {
    http: reqwest::Client,
}

impl HttpTokenFetcher {
    pub fn new(http: reqwest::Client) -> Self {
        Self { http }
    }
}

impl TokenFetcher for HttpTokenFetcher {
    async fn fetch(&self, creds: &ClientCredentials) -> Result<FetchedToken, VuAuthError> {
        let mut form = vec![
            ("grant_type", "client_credentials"),
            ("client_id", creds.client_id.as_str()),
            ("client_secret", creds.client_secret.as_str()),
        ];
        if let Some(scope) = creds.scope.as_deref() {
            form.push(("scope", scope));
        }
        let resp = self
            .http
            .post(&creds.token_url)
            .form(&form)
            .send()
            .await
            .map_err(|e| VuAuthError::TokenRequest(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(VuAuthError::TokenRequest(format!(
                "token endpoint returned {}",
                resp.status()
            )));
        }
        let body: TokenResponse = resp
            .json()
            .await
            .map_err(|e| VuAuthError::TokenParse(e.to_string()))?;
        Ok(FetchedToken {
            access_token: body.access_token,
            lifetime: Duration::seconds(body.expires_in.unwrap_or(DEFAULT_LIFETIME_SECS)),
        })
    }
}

#[derive(Debug, Clone)]
struct Cached {
    token: String,
    expires_at: DateTime<Utc>,
}

/// Caches a client-credentials bearer token, refreshing it shortly before expiry.
pub struct TokenProvider<F = HttpTokenFetcher, C = SystemClock> {
    creds: ClientCredentials,
    fetcher: F,
    clock: C,
    skew: Duration,
    cache: Mutex<Option<Cached>>,
}

impl TokenProvider<HttpTokenFetcher, SystemClock> {
    /// Build a provider backed by the real HTTP fetcher and the wall clock.
    pub fn new(creds: ClientCredentials, http: reqwest::Client) -> Arc<Self> {
        Arc::new(Self::with_parts(
            creds,
            HttpTokenFetcher::new(http),
            SystemClock,
            Duration::seconds(DEFAULT_SKEW_SECS),
        ))
    }
}

impl<F: TokenFetcher, C: Clock> TokenProvider<F, C> {
    pub fn with_parts(creds: ClientCredentials, fetcher: F, clock: C, skew: Duration) -> Self {
        Self {
            creds,
            fetcher,
            clock,
            skew,
            cache: Mutex::new(None),
        }
    }

    /// Return a valid bearer token, fetching (and caching) a fresh one only if none is cached or the
    /// cached one is within `skew` of expiry.
    pub async fn token(&self) -> Result<String, VuAuthError> {
        let now = self.clock.now();
        let mut guard = self.cache.lock().await;
        if let Some(cached) = guard.as_ref()
            && now + self.skew < cached.expires_at
        {
            return Ok(cached.token.clone());
        }
        let fetched = self.fetcher.fetch(&self.creds).await?;
        let token = fetched.access_token;
        *guard = Some(Cached {
            token: token.clone(),
            expires_at: now + fetched.lifetime,
        });
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeClock {
        now: StdMutex<DateTime<Utc>>,
    }
    impl FakeClock {
        fn new(t: DateTime<Utc>) -> Self {
            Self {
                now: StdMutex::new(t),
            }
        }
        fn advance(&self, by: Duration) {
            let mut g = self.now.lock().unwrap();
            *g += by;
        }
    }
    impl Clock for FakeClock {
        fn now(&self) -> DateTime<Utc> {
            *self.now.lock().unwrap()
        }
    }

    struct FakeFetcher {
        calls: AtomicUsize,
        lifetime: Duration,
    }
    impl TokenFetcher for FakeFetcher {
        async fn fetch(&self, _creds: &ClientCredentials) -> Result<FetchedToken, VuAuthError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(FetchedToken {
                access_token: format!("tok-{n}"),
                lifetime: self.lifetime,
            })
        }
    }

    fn creds() -> ClientCredentials {
        ClientCredentials {
            token_url: "https://idp.example/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scope: Some("tads-api/write".into()),
        }
    }

    #[tokio::test]
    async fn caches_within_validity_and_refreshes_after_expiry() {
        let clock = FakeClock::new(Utc::now());
        let fetcher = FakeFetcher {
            calls: AtomicUsize::new(0),
            lifetime: Duration::seconds(3600),
        };
        let provider = TokenProvider::with_parts(creds(), fetcher, clock, Duration::seconds(60));

        // First call fetches.
        assert_eq!(provider.token().await.unwrap(), "tok-0");
        // Second call (still valid) returns the cached token — no new fetch.
        assert_eq!(provider.token().await.unwrap(), "tok-0");

        // Advance past expiry minus skew; the next call must refresh.
        provider.clock.advance(Duration::seconds(3600));
        assert_eq!(provider.token().await.unwrap(), "tok-1");
    }

    #[tokio::test]
    async fn refreshes_inside_skew_window() {
        let clock = FakeClock::new(Utc::now());
        let fetcher = FakeFetcher {
            calls: AtomicUsize::new(0),
            lifetime: Duration::seconds(100),
        };
        let provider = TokenProvider::with_parts(creds(), fetcher, clock, Duration::seconds(60));

        assert_eq!(provider.token().await.unwrap(), "tok-0");
        // 50s later: within 60s skew of the 100s expiry -> treated as stale, refresh.
        provider.clock.advance(Duration::seconds(50));
        assert_eq!(provider.token().await.unwrap(), "tok-1");
    }
}
