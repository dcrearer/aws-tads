//! HTTP API (v2) Lambda authorizer for TADS.
//!
//! Validates the OAuth2 `client_credentials` **access token** minted by Cognito: verifies the
//! RS256 signature against the pool's JWKS, the issuer, expiry, `token_use == access`, the
//! `client_id` (when configured), and that the token carries a `tads-api/*` scope. The native
//! Cognito JWT authorizer would do the same for free; this exists for parity with the TAMS
//! architecture and as a seam for custom authorization logic later.

use std::sync::OnceLock;

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;

/// Claims we care about from a Cognito access token.
#[derive(Debug, Deserialize)]
pub struct Claims {
    pub iss: String,
    pub exp: usize,
    #[serde(default)]
    pub token_use: String,
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub sub: String,
}

/// Static configuration resolved once from the environment.
pub struct Config {
    /// `https://cognito-idp.<region>.amazonaws.com/<poolId>`
    pub issuer: String,
    /// If set, the token's `client_id` must match this exactly.
    pub client_id: Option<String>,
}

/// Extract the raw JWT from an `Authorization` header value (`Bearer <jwt>`, any case), or treat
/// a single-token value as the JWT itself. Returns `None` for empty / malformed values.
pub fn bearer_token(raw: &str) -> Option<&str> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    match raw.split_once(' ') {
        Some((scheme, token)) if scheme.eq_ignore_ascii_case("bearer") => {
            let token = token.trim();
            (!token.is_empty()).then_some(token)
        }
        Some(_) => None, // some other scheme (Basic, etc.)
        None if raw.eq_ignore_ascii_case("bearer") => None, // scheme word only, no token
        None => Some(raw), // bare token, no scheme
    }
}

/// True if the space-delimited `scope` claim grants any `tads-api/*` scope.
pub fn has_api_scope(scope: &str) -> bool {
    scope.split_whitespace().any(|s| s.starts_with("tads-api/"))
}

/// Process-wide JWKS cache. Cognito rotates signing keys rarely; a cold-start fetch per container
/// is plenty, and API Gateway caches authorizer results on top of this.
static JWKS: OnceLock<JwkSet> = OnceLock::new();

async fn jwks(issuer: &str) -> anyhow::Result<&'static JwkSet> {
    if let Some(set) = JWKS.get() {
        return Ok(set);
    }
    let url = format!("{issuer}/.well-known/jwks.json");
    let set: JwkSet = reqwest::get(&url).await?.error_for_status()?.json().await?;
    let _ = JWKS.set(set); // first writer wins; concurrent set() is harmless
    Ok(JWKS.get().expect("jwks just set"))
}

/// Verify a token end-to-end. Returns the validated claims, or an error describing why it was
/// rejected (the caller denies on any error).
pub async fn verify(token: &str, cfg: &Config) -> anyhow::Result<Claims> {
    let kid = decode_header(token)?
        .kid
        .ok_or_else(|| anyhow::anyhow!("token header has no kid"))?;
    let set = jwks(&cfg.issuer).await?;
    let jwk = set
        .find(&kid)
        .ok_or_else(|| anyhow::anyhow!("no JWKS key for kid {kid}"))?;
    let key = DecodingKey::from_jwk(jwk)?;

    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[&cfg.issuer]);
    validation.validate_aud = false; // Cognito access tokens carry `client_id`, not `aud`

    let claims = decode::<Claims>(token, &key, &validation)?.claims;
    if claims.token_use != "access" {
        anyhow::bail!("token_use is `{}`, expected `access`", claims.token_use);
    }
    if let Some(expected) = &cfg.client_id
        && &claims.client_id != expected
    {
        anyhow::bail!("client_id mismatch");
    }
    if !has_api_scope(&claims.scope) {
        anyhow::bail!("token lacks a tads-api/* scope");
    }
    Ok(claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_parsing() {
        assert_eq!(bearer_token("Bearer abc.def.ghi"), Some("abc.def.ghi"));
        assert_eq!(bearer_token("bearer   abc"), Some("abc"));
        assert_eq!(bearer_token("abc.def.ghi"), Some("abc.def.ghi")); // bare token
        assert_eq!(bearer_token("  "), None);
        assert_eq!(bearer_token(""), None);
        assert_eq!(bearer_token("Basic dXNlcjpwdw=="), None);
        assert_eq!(bearer_token("Bearer "), None);
    }

    #[test]
    fn scope_matching() {
        assert!(has_api_scope("tads-api/read tads-api/write"));
        assert!(has_api_scope("openid tads-api/delete"));
        assert!(!has_api_scope("openid profile"));
        assert!(!has_api_scope(""));
    }
}
