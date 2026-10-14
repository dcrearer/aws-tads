//! The TADS write surface the pipeline depends on, as a trait (fakeable) plus a reqwest-backed
//! implementation (the I/O edge).

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tads_domain::{
    DeleteStatus, EventPostResponses, PayloadSchema, TadsId, Track, TrackDeleteRequest, TrackEvent,
};
use vu_auth::TokenProvider;

use crate::error::TadsError;
use crate::events::{EventOutcome, interpret};

/// A handle to an async delete request (`202 Accepted` + `Location`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteHandle {
    /// The `Location` as returned by TADS (path such as `/track-delete-requests/{id}` or absolute).
    pub location: String,
}

/// Write surface of TADS used by the analysis worker.
pub trait TadsApi: Send + Sync {
    /// Resolve a Payload Schema id by its `external_id` (e.g. a pre-seeded `vu:key-moment:v1`).
    /// Returns `None` if no schema with that external id exists.
    fn resolve_schema_id(
        &self,
        external_id: &str,
    ) -> impl Future<Output = Result<Option<TadsId>, TadsError>> + Send;

    fn ensure_schema(
        &self,
        schema: &PayloadSchema,
    ) -> impl Future<Output = Result<(), TadsError>> + Send;

    fn ensure_track(&self, track: &Track) -> impl Future<Output = Result<(), TadsError>> + Send;

    fn post_events(
        &self,
        track_id: TadsId,
        events: &[TrackEvent],
    ) -> impl Future<Output = Result<Vec<EventOutcome>, TadsError>> + Send;

    /// Segment-scoped delete by ids. `Ok(None)` means there was nothing to delete (no async job).
    fn delete_events_by_ids(
        &self,
        track_id: TadsId,
        ids: &[TadsId],
    ) -> impl Future<Output = Result<Option<DeleteHandle>, TadsError>> + Send;

    /// Poll a delete request until it reaches `done` (or `error`/timeout).
    fn wait_delete_complete(
        &self,
        handle: &DeleteHandle,
    ) -> impl Future<Output = Result<(), TadsError>> + Send;
}

/// Resolve a possibly-relative `Location` against the API base URL.
pub fn absolutize(base_url: &str, location: &str) -> String {
    if location.starts_with("http://") || location.starts_with("https://") {
        location.to_string()
    } else {
        format!(
            "{}/{}",
            base_url.trim_end_matches('/'),
            location.trim_start_matches('/')
        )
    }
}

/// reqwest-backed TADS client. All calls carry a bearer token when a [`TokenProvider`] is set.
#[derive(Clone)]
pub struct TadsClient {
    http: reqwest::Client,
    base_url: String,
    token: Option<Arc<TokenProvider>>,
    poll_attempts: u32,
    poll_interval: Duration,
}

impl TadsClient {
    /// `base_url` is the `https://` TADS API root.
    pub fn new(http: reqwest::Client, base_url: impl Into<String>) -> Self {
        Self {
            http,
            base_url: base_url.into(),
            token: None,
            poll_attempts: 30,
            poll_interval: Duration::from_millis(500),
        }
    }

    pub fn with_token(mut self, token: Arc<TokenProvider>) -> Self {
        self.token = Some(token);
        self
    }

    /// Override the delete-request poll schedule (default 30 × 500ms).
    pub fn with_poll(mut self, attempts: u32, interval: Duration) -> Self {
        self.poll_attempts = attempts;
        self.poll_interval = interval;
        self
    }

    fn root(&self) -> &str {
        self.base_url.trim_end_matches('/')
    }

    async fn authorize(
        &self,
        rb: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, TadsError> {
        match &self.token {
            Some(tp) => {
                let token = tp
                    .token()
                    .await
                    .map_err(|e| TadsError::Auth(e.to_string()))?;
                Ok(rb.bearer_auth(token))
            }
            None => Ok(rb),
        }
    }
}

impl TadsApi for TadsClient {
    async fn resolve_schema_id(&self, external_id: &str) -> Result<Option<TadsId>, TadsError> {
        let url = format!("{}/schemas", self.root());
        let resp = self
            .authorize(self.http.get(&url))
            .await?
            .send()
            .await
            .map_err(|e| TadsError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(TadsError::Status {
                status: status.as_u16(),
                url,
            });
        }
        let schemas: Vec<PayloadSchema> = resp
            .json()
            .await
            .map_err(|e| TadsError::Decode(e.to_string()))?;
        Ok(schemas
            .into_iter()
            .find(|s| s.external_id.as_deref() == Some(external_id))
            .map(|s| s.id))
    }

    async fn ensure_schema(&self, schema: &PayloadSchema) -> Result<(), TadsError> {
        let url = format!("{}/schemas/{}", self.root(), schema.id);
        let resp = self
            .authorize(self.http.put(&url).json(schema))
            .await?
            .send()
            .await
            .map_err(|e| TadsError::Http(e.to_string()))?;
        expect_success(resp.status(), url)
    }

    async fn ensure_track(&self, track: &Track) -> Result<(), TadsError> {
        let url = format!("{}/tracks/{}", self.root(), track.id);
        let resp = self
            .authorize(self.http.put(&url).json(track))
            .await?
            .send()
            .await
            .map_err(|e| TadsError::Http(e.to_string()))?;
        expect_success(resp.status(), url)
    }

    async fn post_events(
        &self,
        track_id: TadsId,
        events: &[TrackEvent],
    ) -> Result<Vec<EventOutcome>, TadsError> {
        let url = format!("{}/tracks/{track_id}/events", self.root());
        let resp = self
            .authorize(self.http.post(&url).json(events))
            .await?
            .send()
            .await
            .map_err(|e| TadsError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(TadsError::Status {
                status: status.as_u16(),
                url,
            });
        }
        let parsed: EventPostResponses = resp
            .json()
            .await
            .map_err(|e| TadsError::Decode(e.to_string()))?;
        Ok(interpret(&parsed))
    }

    async fn delete_events_by_ids(
        &self,
        track_id: TadsId,
        ids: &[TadsId],
    ) -> Result<Option<DeleteHandle>, TadsError> {
        if ids.is_empty() {
            return Ok(None);
        }
        let joined = ids
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let url = format!("{}/tracks/{track_id}/events", self.root());
        let resp = self
            .authorize(self.http.delete(&url).query(&[("ids", joined)]))
            .await?
            .send()
            .await
            .map_err(|e| TadsError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(TadsError::Status {
                status: status.as_u16(),
                url,
            });
        }
        // 202 carries a Location to the async delete-request; a plain 2xx means nothing to delete.
        Ok(resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(|loc| DeleteHandle {
                location: loc.to_string(),
            }))
    }

    async fn wait_delete_complete(&self, handle: &DeleteHandle) -> Result<(), TadsError> {
        let url = absolutize(self.root(), &handle.location);
        for _ in 0..self.poll_attempts {
            let resp = self
                .authorize(self.http.get(&url))
                .await?
                .send()
                .await
                .map_err(|e| TadsError::Http(e.to_string()))?;
            let status = resp.status();
            if !status.is_success() {
                return Err(TadsError::Status {
                    status: status.as_u16(),
                    url,
                });
            }
            let req: TrackDeleteRequest = resp
                .json()
                .await
                .map_err(|e| TadsError::Decode(e.to_string()))?;
            match req.status {
                DeleteStatus::Done => return Ok(()),
                DeleteStatus::Error => {
                    return Err(TadsError::DeleteFailed(
                        req.error.map(|e| e.summary).unwrap_or_default(),
                    ));
                }
                DeleteStatus::Created | DeleteStatus::Started => {
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
        Err(TadsError::DeleteTimeout {
            attempts: self.poll_attempts,
        })
    }
}

fn expect_success(status: reqwest::StatusCode, url: String) -> Result<(), TadsError> {
    if status.is_success() {
        Ok(())
    } else {
        Err(TadsError::Status {
            status: status.as_u16(),
            url,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolutize_handles_path_and_absolute() {
        assert_eq!(
            absolutize("https://tads.example.com", "/track-delete-requests/abc"),
            "https://tads.example.com/track-delete-requests/abc"
        );
        assert_eq!(
            absolutize("https://tads.example.com/", "track-delete-requests/abc"),
            "https://tads.example.com/track-delete-requests/abc"
        );
        assert_eq!(
            absolutize("https://tads.example.com", "https://other/abs"),
            "https://other/abs"
        );
    }
}
