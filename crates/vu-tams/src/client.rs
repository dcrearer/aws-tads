//! The TAMS HTTP surface the pipeline depends on, as a trait (so the worker is testable with a
//! fake) plus a reqwest-backed implementation (the I/O edge).

use std::future::Future;
use std::sync::Arc;

use serde::Deserialize;
use vu_auth::TokenProvider;

use crate::error::TamsError;
use crate::models::FlowSegment;

/// Read surface of TAMS used by the analysis worker.
pub trait TamsApi: Send + Sync {
    /// Fetch a flow's segments, optionally restricted to a TADS/TAMS `timerange` string.
    fn get_flow_segments(
        &self,
        flow_id: &str,
        timerange: Option<&str>,
    ) -> impl Future<Output = Result<Vec<FlowSegment>, TamsError>> + Send;

    /// Resolve the Source id a flow belongs to (preferred for the Track's `tams://` reference).
    fn resolve_source_id(
        &self,
        flow_id: &str,
    ) -> impl Future<Output = Result<Option<String>, TamsError>> + Send;

    /// Download segment media from a (typically pre-signed) URL.
    fn fetch_media(&self, url: &str) -> impl Future<Output = Result<Vec<u8>, TamsError>> + Send;
}

#[derive(Debug, Deserialize)]
struct FlowDetail {
    #[serde(default)]
    source_id: Option<String>,
}

/// reqwest-backed TAMS client. Control-plane calls (`get_flow_segments`, `resolve_source_id`) carry
/// a bearer token when a [`TokenProvider`] is configured; media fetches hit pre-signed URLs
/// directly and are unauthenticated.
#[derive(Clone)]
pub struct TamsClient {
    http: reqwest::Client,
    base_url: String,
    token: Option<Arc<TokenProvider>>,
}

impl TamsClient {
    /// `base_url` is the `https://` TAMS API root (e.g. `https://tams.example.com`).
    pub fn new(http: reqwest::Client, base_url: impl Into<String>) -> Self {
        Self {
            http,
            base_url: base_url.into(),
            token: None,
        }
    }

    /// Attach an OAuth2 token provider for authenticated control-plane calls.
    pub fn with_token(mut self, token: Arc<TokenProvider>) -> Self {
        self.token = Some(token);
        self
    }

    fn root(&self) -> &str {
        self.base_url.trim_end_matches('/')
    }

    async fn authorize(
        &self,
        rb: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, TamsError> {
        match &self.token {
            Some(tp) => {
                let token = tp
                    .token()
                    .await
                    .map_err(|e| TamsError::Auth(e.to_string()))?;
                Ok(rb.bearer_auth(token))
            }
            None => Ok(rb),
        }
    }
}

impl TamsApi for TamsClient {
    async fn get_flow_segments(
        &self,
        flow_id: &str,
        timerange: Option<&str>,
    ) -> Result<Vec<FlowSegment>, TamsError> {
        let url = format!("{}/flows/{flow_id}/segments", self.root());
        let mut rb = self.http.get(&url);
        if let Some(tr) = timerange {
            rb = rb.query(&[("timerange", tr)]);
        }
        let resp = self
            .authorize(rb)
            .await?
            .send()
            .await
            .map_err(|e| TamsError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(TamsError::Status {
                status: status.as_u16(),
                url,
            });
        }
        resp.json::<Vec<FlowSegment>>()
            .await
            .map_err(|e| TamsError::Decode(e.to_string()))
    }

    async fn resolve_source_id(&self, flow_id: &str) -> Result<Option<String>, TamsError> {
        let url = format!("{}/flows/{flow_id}", self.root());
        let resp = self
            .authorize(self.http.get(&url))
            .await?
            .send()
            .await
            .map_err(|e| TamsError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(TamsError::Status {
                status: status.as_u16(),
                url,
            });
        }
        let detail = resp
            .json::<FlowDetail>()
            .await
            .map_err(|e| TamsError::Decode(e.to_string()))?;
        Ok(detail.source_id)
    }

    async fn fetch_media(&self, url: &str) -> Result<Vec<u8>, TamsError> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| TamsError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(TamsError::Status {
                status: status.as_u16(),
                url: url.to_string(),
            });
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| TamsError::Http(e.to_string()))?;
        Ok(bytes.to_vec())
    }
}
