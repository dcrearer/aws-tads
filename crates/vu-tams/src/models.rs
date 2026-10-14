//! TAMS webhook + segment models (the subset the pipeline consumes).

use serde::{Deserialize, Serialize};
use tads_domain::TimeRange;
use tads_domain::timerange::Timestamp;

/// Payload of the TAMS `flows/segments_added` webhook. Unknown fields are ignored so the model is
/// resilient to TAMS adding keys.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SegmentsAddedEvent {
    pub flow_id: String,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub segments: Vec<FlowSegment>,
}

/// A single immutable TAMS flow segment and where its media can be fetched.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowSegment {
    pub object_id: String,
    /// Absolute TAI timerange of this segment on the flow timeline.
    pub timerange: TimeRange,
    #[serde(default)]
    pub get_urls: Vec<GetUrl>,
}

/// A fetchable media location for a segment. TAMS lists several per segment — an unauthenticated
/// bucket URL and a pre-signed one; prefer the pre-signed (directly fetchable) variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetUrl {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presigned: Option<bool>,
}

impl FlowSegment {
    /// The immutable TAI start of the segment — the anchor used to derive stable Event ids. `None`
    /// for an unbounded-start range (should not occur for a real segment).
    pub fn segment_anchor(&self) -> Option<Timestamp> {
        self.timerange.start.map(|b| b.ts)
    }

    /// First media URL for the segment, if any (raw list order).
    pub fn first_get_url(&self) -> Option<&str> {
        self.get_urls.first().map(|g| g.url.as_str())
    }

    /// The media URL to fetch: prefer a pre-signed entry (`presigned: true`, or a label containing
    /// `presigned`), since TAMS lists the unauthenticated bucket URL first. Falls back to the first.
    pub fn media_url(&self) -> Option<&str> {
        self.get_urls
            .iter()
            .find(|g| g.presigned == Some(true))
            .or_else(|| {
                self.get_urls
                    .iter()
                    .find(|g| g.label.as_deref().is_some_and(|l| l.contains("presigned")))
            })
            .or_else(|| self.get_urls.first())
            .map(|g| g.url.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_webhook_payload_ignoring_unknown_fields() {
        let json = serde_json::json!({
            "event_type": "flows/segments_added",
            "flow_id": "f-1",
            "source_id": "s-1",
            "segments": [
                {
                    "object_id": "obj-1",
                    "timerange": "[10:0_12:0)",
                    "get_urls": [{ "url": "https://media/obj-1", "label": "video/mp4" }],
                    "key_frame_count": 3
                }
            ]
        });
        let ev: SegmentsAddedEvent = serde_json::from_value(json).unwrap();
        assert_eq!(ev.flow_id, "f-1");
        assert_eq!(ev.source_id.as_deref(), Some("s-1"));
        assert_eq!(ev.segments.len(), 1);
        let seg = &ev.segments[0];
        assert_eq!(seg.object_id, "obj-1");
        assert_eq!(seg.segment_anchor(), Some(Timestamp::new(10, 0)));
        assert_eq!(seg.first_get_url(), Some("https://media/obj-1"));
    }

    #[test]
    fn media_url_prefers_presigned() {
        // TAMS lists the unauthenticated bucket URL first, the pre-signed one second.
        let json = serde_json::json!({
            "flow_id": "f-1",
            "segments": [{
                "object_id": "obj-1",
                "timerange": "[0:0_3:0)",
                "get_urls": [
                    { "url": "https://bucket/raw", "label": "aws.us-east-1:s3:tams" },
                    { "url": "https://bucket/signed", "presigned": true, "label": "aws.us-east-1:s3.presigned:tams" }
                ]
            }]
        });
        let ev: SegmentsAddedEvent = serde_json::from_value(json).unwrap();
        let seg = &ev.segments[0];
        assert_eq!(seg.first_get_url(), Some("https://bucket/raw"));
        assert_eq!(seg.media_url(), Some("https://bucket/signed"));
    }

    #[test]
    fn source_id_is_optional() {
        let json = serde_json::json!({ "flow_id": "f-1", "segments": [] });
        let ev: SegmentsAddedEvent = serde_json::from_value(json).unwrap();
        assert!(ev.source_id.is_none());
    }
}
