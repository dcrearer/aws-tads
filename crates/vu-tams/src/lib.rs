//! TAMS-side client for the write path: verify the inbound `flows/segments_added` webhook, parse
//! its payload into the fields the analyzer needs, and fetch the segment's media bytes.
//!
//! The segment `timerange` is the authoritative absolute clock (its start is the `segment_anchor`
//! used for deterministic event ids). Presigned media URLs are fetched without auth; TAMS-native
//! URLs use the shared [`vu_auth::TokenProvider`]. See docs/inference-plan.md §2a.

use serde::Deserialize;
use tads_domain::{TimeRange, Timestamp};
use vu_auth::TokenProvider;

#[derive(Debug, thiserror::Error)]
pub enum VuTamsError {
    #[error("TAMS request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("auth: {0}")]
    Auth(#[from] vu_auth::AuthError),
    #[error("TAMS GET {url} returned {status}")]
    UnexpectedStatus { url: String, status: u16 },
}

/// Verify an inbound TAMS webhook's shared-secret header in constant time.
///
/// `provided` is the value of the agreed header (e.g. `x-api-key`) on the request, or `None` if it
/// was absent. The compare is constant-time over equal-length inputs so a valid key is not
/// distinguishable byte-by-byte by timing. (Length is not treated as secret.)
pub fn verify_api_key(expected: &str, provided: Option<&str>) -> bool {
    match provided {
        None => false,
        Some(p) => constant_time_eq(expected.as_bytes(), p.as_bytes()),
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Payload of a TAMS `flows/segments_added` webhook — the fields this pipeline consumes.
#[derive(Debug, Clone, Deserialize)]
pub struct SegmentsAddedEvent {
    pub flow_id: String,
    /// Rendition-independent Source id; present when TAMS includes it.
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub segments: Vec<FlowSegment>,
}

/// One newly-added Flow Segment.
#[derive(Debug, Clone, Deserialize)]
pub struct FlowSegment {
    /// Storage object id for the segment's media essence.
    pub object_id: String,
    /// Absolute TAI span of the segment on the media timeline.
    pub timerange: TimeRange,
    /// Presigned (or TAMS-native) URLs to fetch the media object.
    #[serde(default)]
    pub get_urls: Vec<GetUrl>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetUrl {
    pub url: String,
    #[serde(default)]
    pub label: Option<String>,
}

impl FlowSegment {
    /// The segment's immutable start instant — the `segment_anchor` for deterministic event ids.
    /// `None` only for an unbounded-start timerange (not expected for a real segment).
    pub fn segment_anchor(&self) -> Option<Timestamp> {
        self.timerange.start.map(|b| b.ts)
    }

    /// First media URL to fetch, if any.
    pub fn first_get_url(&self) -> Option<&str> {
        self.get_urls.first().map(|g| g.url.as_str())
    }
}

/// Client for fetching TAMS media.
#[derive(Clone)]
pub struct TamsClient {
    http: reqwest::Client,
    tokens: TokenProvider,
}

impl TamsClient {
    pub fn new(http: reqwest::Client, tokens: TokenProvider) -> Self {
        Self { http, tokens }
    }

    /// Fetch a presigned media URL (no auth — the signature carries authorization).
    #[tracing::instrument(skip(self, url))]
    pub async fn fetch_media(&self, url: &str) -> Result<Vec<u8>, VuTamsError> {
        self.get_bytes(self.http.get(url), url).await
    }

    /// Fetch a TAMS-native URL that requires a Bearer token (non-presigned).
    #[tracing::instrument(skip(self, url))]
    pub async fn fetch_media_authed(&self, url: &str) -> Result<Vec<u8>, VuTamsError> {
        let token = self.tokens.token().await?;
        self.get_bytes(self.http.get(url).bearer_auth(token), url)
            .await
    }

    async fn get_bytes(
        &self,
        req: reqwest::RequestBuilder,
        url: &str,
    ) -> Result<Vec<u8>, VuTamsError> {
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(VuTamsError::UnexpectedStatus {
                url: url.to_string(),
                status: status.as_u16(),
            });
        }
        Ok(resp.bytes().await?.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_verification() {
        assert!(verify_api_key("s3cr3t", Some("s3cr3t")));
        assert!(!verify_api_key("s3cr3t", Some("wrong")));
        assert!(!verify_api_key("s3cr3t", Some("s3cr3t-longer")));
        assert!(!verify_api_key("s3cr3t", None));
    }

    #[test]
    fn parses_segments_added_payload() {
        let raw = r#"{
            "flow_id": "f87f7f32-a34d-4a6f-b085-a5ee49b4b2f8",
            "source_id": "7abe4d59-7d1c-432d-ba2a-6180e12137cc",
            "segments": [{
                "object_id": "obj-1",
                "timerange": "[1694429247:0_1694429251:0)",
                "get_urls": [{ "url": "https://store.example/obj-1?sig=abc", "label": "proxy" }]
            }]
        }"#;
        let ev: SegmentsAddedEvent = serde_json::from_str(raw).unwrap();
        assert_eq!(
            ev.source_id.as_deref(),
            Some("7abe4d59-7d1c-432d-ba2a-6180e12137cc")
        );
        let seg = &ev.segments[0];
        assert_eq!(seg.object_id, "obj-1");
        assert_eq!(seg.segment_anchor(), Some(Timestamp::new(1_694_429_247, 0)));
        assert_eq!(
            seg.first_get_url(),
            Some("https://store.example/obj-1?sig=abc")
        );
    }

    #[test]
    fn tolerates_missing_optional_fields() {
        let raw = r#"{ "flow_id": "f1", "segments": [] }"#;
        let ev: SegmentsAddedEvent = serde_json::from_str(raw).unwrap();
        assert!(ev.source_id.is_none());
        assert!(ev.segments.is_empty());
    }
}
