//! TADS HTTP client for the write path: resolve+cache Payload Schemas, ensure Tracks, POST Events
//! (batched, with per-event outcome), and delete Events for reanalysis.
//!
//! TADS owns validation; this client maps already-built payloads onto the wire and reports back
//! what the store said (notably per-event `400`s). All calls are Bearer-authenticated via the
//! shared [`vu_auth::TokenProvider`]. See docs/inference-plan.md §5.

use std::collections::HashMap;
use std::sync::Arc;

use tads_domain::{
    EventPostResponse, EventPostResponses, PayloadSchema, TadsId, Track, TrackEventPost,
};
use tokio::sync::Mutex;
use vu_auth::TokenProvider;

#[derive(Debug, thiserror::Error)]
pub enum VuTadsError {
    #[error("TADS request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("auth: {0}")]
    Auth(#[from] vu_auth::AuthError),
    #[error("no TADS Payload Schema found for external_id `{0}` (seed it before running)")]
    SchemaNotFound(String),
    #[error("TADS {operation} returned unexpected status {status}: {body}")]
    UnexpectedStatus {
        operation: &'static str,
        status: u16,
        body: String,
    },
}

/// Outcome of a (possibly batched) event POST. `responses` carries the per-event detail when TADS
/// returns it, so callers can route per-event `400`s without failing the whole batch.
#[derive(Debug, Clone)]
pub struct EventPostOutcome {
    pub status: u16,
    pub responses: Option<EventPostResponses>,
}

impl EventPostOutcome {
    /// True when every event in the batch was accepted.
    pub fn all_successful(&self) -> bool {
        match &self.responses {
            Some(r) => r.all_successful,
            // No per-event body: trust a 2xx overall status.
            None => (200..300).contains(&self.status),
        }
    }

    /// The events TADS rejected (per-event non-2xx), for logging/metrics/retry decisions.
    pub fn failures(&self) -> Vec<&EventPostResponse> {
        self.responses
            .as_ref()
            .map(|r| {
                r.responses
                    .iter()
                    .filter(|e| !(200..300).contains(&e.http_status_code))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Outcome of a delete. TADS may complete synchronously or hand back a `202` with a `Location`
/// pointing at an async Track Delete Request.
#[derive(Debug, Clone)]
pub struct DeleteOutcome {
    pub status: u16,
    pub async_request: Option<String>,
}

/// Client for one TADS service.
#[derive(Clone)]
pub struct TadsClient {
    http: reqwest::Client,
    base_url: String,
    tokens: TokenProvider,
    schema_cache: Arc<Mutex<HashMap<String, TadsId>>>,
}

impl TadsClient {
    /// `base_url` is the API root (any trailing slash is trimmed).
    pub fn new(http: reqwest::Client, base_url: impl Into<String>, tokens: TokenProvider) -> Self {
        Self {
            http,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            tokens,
            schema_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn bearer(&self) -> Result<String, VuTadsError> {
        Ok(self.tokens.token().await?)
    }

    /// Resolve a Payload Schema `external_id` (e.g. `vu:key-moment:v1`) to its `schema_id`, caching
    /// the result. Errors if no schema is registered — schemas are seeded out of band.
    #[tracing::instrument(skip(self), fields(external_id))]
    pub async fn resolve_schema(&self, external_id: &str) -> Result<TadsId, VuTadsError> {
        if let Some(id) = self.schema_cache.lock().await.get(external_id) {
            return Ok(*id);
        }
        let token = self.bearer().await?;
        let resp = self
            .http
            .get(format!("{}/schemas", self.base_url))
            .bearer_auth(token)
            .query(&[("external_id", external_id)])
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(VuTadsError::UnexpectedStatus {
                operation: "GET /schemas",
                status: status.as_u16(),
                body: resp.text().await.unwrap_or_default(),
            });
        }
        let schemas: Vec<PayloadSchema> = resp.json().await?;
        let id = schemas
            .into_iter()
            .next()
            .map(|s| s.id)
            .ok_or_else(|| VuTadsError::SchemaNotFound(external_id.to_string()))?;
        self.schema_cache
            .lock()
            .await
            .insert(external_id.to_string(), id);
        Ok(id)
    }

    /// Startup self-check: resolve every required schema, failing fast if any is missing.
    pub async fn ensure_schemas(&self, external_ids: &[&str]) -> Result<(), VuTadsError> {
        for id in external_ids {
            self.resolve_schema(id).await?;
        }
        Ok(())
    }

    /// Create or replace a Track (`PUT /tracks/{id}`). Idempotent — the Track's `id` is a
    /// deterministic function of (Source, analysis-type).
    #[tracing::instrument(skip(self, track), fields(track_id = %track.id))]
    pub async fn put_track(&self, track: &Track) -> Result<(), VuTadsError> {
        let token = self.bearer().await?;
        let resp = self
            .http
            .put(format!("{}/tracks/{}", self.base_url, track.id))
            .bearer_auth(token)
            .json(track)
            .send()
            .await?;
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(VuTadsError::UnexpectedStatus {
                operation: "PUT /tracks/{id}",
                status: status.as_u16(),
                body: resp.text().await.unwrap_or_default(),
            })
        }
    }

    /// POST a batch of Events to a Track. Returns the per-event outcome; a per-event `400` does not
    /// fail the call (inspect [`EventPostOutcome::failures`]).
    #[tracing::instrument(skip(self, events), fields(track_id = %track_id, count = events.len()))]
    pub async fn post_events(
        &self,
        track_id: TadsId,
        events: &[TrackEventPost],
    ) -> Result<EventPostOutcome, VuTadsError> {
        let token = self.bearer().await?;
        let resp = self
            .http
            .post(format!("{}/tracks/{}/events", self.base_url, track_id))
            .bearer_auth(token)
            .json(events)
            .send()
            .await?;
        let status = resp.status().as_u16();
        // The body is per-event responses on 2xx and often on 207/4xx too; tolerate its absence.
        let responses = resp.json::<EventPostResponses>().await.ok();
        Ok(EventPostOutcome { status, responses })
    }

    /// Delete Events on a Track by id (`DELETE /tracks/{id}/events?ids=…`) — the reanalysis path.
    #[tracing::instrument(skip(self, ids), fields(track_id = %track_id, count = ids.len()))]
    pub async fn delete_events_by_ids(
        &self,
        track_id: TadsId,
        ids: &[TadsId],
    ) -> Result<DeleteOutcome, VuTadsError> {
        let token = self.bearer().await?;
        let joined = ids
            .iter()
            .map(TadsId::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let resp = self
            .http
            .delete(format!("{}/tracks/{}/events", self.base_url, track_id))
            .bearer_auth(token)
            .query(&[("ids", joined)])
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(VuTadsError::UnexpectedStatus {
                operation: "DELETE /tracks/{id}/events",
                status: status.as_u16(),
                body: resp.text().await.unwrap_or_default(),
            });
        }
        // 202 Accepted → an async delete request whose location we surface for polling.
        let async_request = (status.as_u16() == 202)
            .then(|| {
                resp.headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            })
            .flatten();
        Ok(DeleteOutcome {
            status: status.as_u16(),
            async_request,
        })
    }
}
