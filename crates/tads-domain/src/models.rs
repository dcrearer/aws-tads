//! Serde models mirroring the `bbc/tads` v0.1 JSON schemas.
//!
//! Conventions: server-managed fields (`created`, `*_updated`, derived `timerange`) are
//! `Option` and skipped when absent; boolean flags carry the spec defaults; `type` fields are
//! renamed to avoid the Rust keyword.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::TadsId;
use crate::tags::Tags;
use crate::timerange::TimeRange;

fn default_true() -> bool {
    true
}

fn is_false(b: &bool) -> bool {
    !*b
}

fn default_denominator() -> u64 {
    1
}

/// `error.json` — error status metadata embedded in delete requests, webhook state, and
/// per-event POST responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    #[serde(rename = "type")]
    pub kind: String,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceback: Option<Vec<String>>,
    pub time: DateTime<Utc>,
}

/// Fixed edit-rate grid for Event timeranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EditRate {
    pub numerator: u64,
    #[serde(default = "default_denominator")]
    pub denominator: u64,
}

/// The external timeline a Track is pinned to (`track.timeline_reference`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineReference {
    /// URI/path/other unique id of the external entity (e.g. a TAMS Flow).
    pub entity_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeline_selected: Option<String>,
}

/// `track.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Track {
    pub id: TadsId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    pub timeline_reference: TimelineReference,
    #[serde(default, skip_serializing_if = "Tags::is_empty")]
    pub tags: Tags,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_updated: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_updated: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
    #[serde(default = "default_true")]
    pub allow_overlaps: bool,
    #[serde(default = "default_true")]
    pub editable_events: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timerange: Option<TimeRange>,
    pub schema_id: TadsId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edit_rate: Option<EditRate>,
}

/// `schema.json` — a managed Payload Schema (JSON Schema) that Events validate against.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PayloadSchema {
    pub id: TadsId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Tags::is_empty")]
    pub tags: Tags,
    /// The JSON Schema document itself. Immutable once set (edits are rejected 403).
    pub schema: Value,
}

/// Stored / GET form of a Track Event (`track-event-get.json`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackEvent {
    pub id: TadsId,
    pub timerange: TimeRange,
    pub payload: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<DateTime<Utc>>,
}

/// POST form of a Track Event (`track-event-post.json`). `id` optional (assigned if omitted;
/// supplying it targets an existing Event for edit/replace — POST is an upsert).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TrackEventPost {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<TadsId>,
    pub timerange: TimeRange,
    pub payload: Value,
}

/// `track-event-post-responses.json` — per-event outcome of a (possibly batched) POST.
///
/// `Deserialize` is derived as well as `Serialize` so an API *client* (e.g. the video-understanding
/// write path) can parse the response it receives, not just the server emit it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventPostResponses {
    pub all_successful: bool,
    pub responses: Vec<EventPostResponse>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventPostResponse {
    pub http_status_code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<TadsId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

/// Status lifecycle of a track delete request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeleteStatus {
    Created,
    Started,
    Done,
    Error,
}

/// `track-delete-request.json` — async deletion job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackDeleteRequest {
    pub id: TadsId,
    pub track_id: TadsId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timerange_to_delete: Option<TimeRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timerange_remaining: Option<TimeRange>,
    pub delete_track: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiry: Option<DateTime<Utc>>,
    pub status: DeleteStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

/// `service.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Service {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub service_type: String,
    pub api_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_version: Option<String>,
}

/// Webhook event types a subscriber may register for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebhookEvent {
    #[serde(rename = "schemas/created")]
    SchemasCreated,
    #[serde(rename = "schemas/updated")]
    SchemasUpdated,
    #[serde(rename = "schemas/deleted")]
    SchemasDeleted,
    #[serde(rename = "tracks/created")]
    TracksCreated,
    #[serde(rename = "tracks/updated")]
    TracksUpdated,
    #[serde(rename = "tracks/deleted")]
    TracksDeleted,
    #[serde(rename = "tracks/events_added")]
    TracksEventsAdded,
    #[serde(rename = "tracks/events_updated")]
    TracksEventsUpdated,
    #[serde(rename = "tracks/events_deleted")]
    TracksEventsDeleted,
}

/// Subscriber-side webhook lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WebhookStatus {
    Created,
    Started,
    Disabled,
    Error,
}

/// Common webhook subscription fields (`webhook.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Webhook {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key_name: Option<String>,
    pub events: Vec<WebhookEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_ids: Option<Vec<TadsId>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_ids: Option<Vec<TadsId>>,
    /// Timeline-reference filter. The spec (and real deployments) use the flat dotted JSON key
    /// `timeline_reference.entity_id`, whose value is an array of entity-id URIs.
    #[serde(
        rename = "timeline_reference.entity_id",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub timeline_reference_entity_id: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub editable_events: Option<bool>,
    #[serde(default, skip_serializing_if = "Tags::is_empty")]
    pub tags: Tags,
}

/// POST/PUT body for registering a webhook (`webhook-post.json` / `webhook-put.json`).
/// Adds the write-only `api_key_value` and an optional desired `status`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct WebhookPost {
    #[serde(flatten)]
    pub webhook: Webhook,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<WebhookStatus>,
}

/// GET form of a webhook (`webhook-get.json`). Never echoes `api_key_value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookGet {
    pub id: TadsId,
    #[serde(flatten)]
    pub webhook: Webhook,
    pub status: WebhookStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn track_defaults_apply_on_deserialize() {
        let t: Track = serde_json::from_value(json!({
            "id": "2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f",
            "timeline_reference": { "entity_id": "tams://flow/abc" },
            "schema_id": "3e2f3c4d-5b6c-4d7e-9faf-1b2c3d4e5f60"
        }))
        .unwrap();
        assert!(!t.read_only);
        assert!(t.allow_overlaps);
        assert!(t.editable_events);
        assert!(t.tags.is_empty());
    }

    #[test]
    fn webhook_event_renames_to_slash_form() {
        let j = serde_json::to_string(&WebhookEvent::TracksEventsAdded).unwrap();
        assert_eq!(j, "\"tracks/events_added\"");
    }

    #[test]
    fn error_body_uses_type_key() {
        let e = ErrorBody {
            kind: "conflict".into(),
            summary: "overlap".into(),
            traceback: None,
            time: Utc::now(),
        };
        let v = serde_json::to_value(&e).unwrap();
        assert!(v.get("type").is_some());
    }
}
