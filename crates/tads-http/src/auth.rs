//! Authentication extractor for the three spec security schemes (bearer JWT / basic /
//! `access_token` query param).
//!
//! Authorization is explicitly **out of scope** of the TADS spec, so this resolves the calling
//! principal (to populate `created_by`/`updated_by`) and defaults to permissive. A real
//! deployment plugs an API Gateway authorizer / Cognito / OAuth in front; this extractor then
//! simply reads the resulting identity.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

/// The authenticated caller, if one could be resolved. `subject` feeds `created_by`/`updated_by`.
#[derive(Debug, Clone, Default)]
pub struct Principal {
    pub subject: Option<String>,
}

impl Principal {
    /// The identity string to stamp onto created/updated resources.
    pub fn actor(&self) -> Option<&str> {
        self.subject.as_deref()
    }
}

impl<S: Sync> FromRequestParts<S> for Principal {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Prefer an upstream-resolved identity header (set by an API Gateway authorizer).
        if let Some(sub) = parts
            .headers
            .get("x-tads-principal")
            .and_then(|v| v.to_str().ok())
        {
            return Ok(Principal {
                subject: Some(sub.to_string()),
            });
        }

        // Otherwise note that a bearer token was presented (we don't verify it here).
        if let Some(auth) = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            && let Some(_token) = auth.strip_prefix("Bearer ")
        {
            return Ok(Principal {
                subject: Some("bearer".to_string()),
            });
        }

        Ok(Principal::default())
    }
}
