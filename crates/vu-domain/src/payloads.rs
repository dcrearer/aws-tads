//! Typed key-moment payload and its wire mapping.
//!
//! The shape mirrors the TADS-seeded `vu:key-moment:v1` Payload Schema exactly (it enforces
//! `additionalProperties:false`, so we must emit only its fields): required `title`, `type`
//! (a fixed enum) and `confidence`, plus optional `description`, `evidence[]` and `provenance`.

use serde::{Deserialize, Serialize};
use tads_domain::{TadsId, TimeRange, TrackEvent};

/// VU external id of the key-moment Payload Schema (seeded in TADS).
pub const KEY_MOMENT_SCHEMA_EXTERNAL_ID: &str = "vu:key-moment:v1";

/// The VU analysis type label (used in ids and Track derivation).
pub const KEY_MOMENT_ANALYSIS_TYPE: &str = "key-moment";

/// The `vu:key-moment:v1` JSON Schema document, vendored so the pipeline can validate payloads
/// locally against the exact gate TADS enforces server-side.
pub const KEY_MOMENT_SCHEMA_JSON: &str = include_str!("../schemas/key-moment.schema.json");

/// The kind of key moment — the schema's `type` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MomentType {
    NotableEvent,
    SpeakerChange,
    Highlight,
    SceneCut,
    TextOnScreen,
}

impl MomentType {
    /// Map a free-form model label onto the enum, defaulting to `NotableEvent` for anything
    /// unrecognised (the model is prompted with the allowed values but may paraphrase).
    pub fn parse_lenient(s: &str) -> Self {
        match s
            .trim()
            .to_ascii_lowercase()
            .replace([' ', '-'], "_")
            .as_str()
        {
            "speaker_change" => Self::SpeakerChange,
            "highlight" => Self::Highlight,
            "scene_cut" | "scene_change" => Self::SceneCut,
            "text_on_screen" | "text" => Self::TextOnScreen,
            _ => Self::NotableEvent,
        }
    }
}

/// Where a payload came from — the schema's optional `provenance` object (`additionalProperties:false`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Provenance {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// RFC 3339 / ISO-8601 timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated_at: Option<String>,
}

/// Typed `vu:key-moment:v1` payload. Serializes to the Event `payload` object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyMomentPayload {
    pub title: String,
    #[serde(rename = "type")]
    pub kind: MomentType,
    pub confidence: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Provenance>,
}

impl KeyMomentPayload {
    /// Build the outbound Track Event. `TrackEvent` serializes to the `{id, timerange, payload}`
    /// POST shape (its `created` field is `None` and omitted).
    pub fn into_track_event(self, id: TadsId, timerange: TimeRange) -> TrackEvent {
        let payload = serde_json::to_value(&self).expect("KeyMomentPayload is always serializable");
        TrackEvent {
            id,
            timerange,
            payload,
            created: None,
        }
    }
}

/// A moment together with its absolute timerange, before id assignment / clipping.
#[derive(Debug, Clone, PartialEq)]
pub struct Moment {
    pub timerange: TimeRange,
    pub payload: KeyMomentPayload,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> KeyMomentPayload {
        KeyMomentPayload {
            title: "Goal".into(),
            kind: MomentType::Highlight,
            confidence: 0.92,
            description: Some("The striker scores from the edge of the box.".into()),
            evidence: vec!["crowd cheering".into()],
            provenance: Some(Provenance {
                model: Some("us.twelvelabs.pegasus-1-5-v1:0".into()),
                model_version: None,
                source: Some("tams://host/sources/abc".into()),
                generated_at: Some("2026-10-13T23:00:00Z".into()),
            }),
        }
    }

    #[test]
    fn type_serializes_snake_case_under_type_key() {
        let v = serde_json::to_value(sample()).unwrap();
        assert_eq!(v["type"], serde_json::json!("highlight"));
        assert!(
            v.get("kind").is_none(),
            "field is wire-named `type`, not `kind`"
        );
    }

    #[test]
    fn parse_lenient_maps_and_defaults() {
        assert_eq!(MomentType::parse_lenient("Scene Cut"), MomentType::SceneCut);
        assert_eq!(
            MomentType::parse_lenient("speaker-change"),
            MomentType::SpeakerChange
        );
        assert_eq!(
            MomentType::parse_lenient("Presentation"),
            MomentType::NotableEvent
        );
    }

    #[test]
    fn optional_fields_are_omitted_when_empty() {
        let p = KeyMomentPayload {
            title: "x".into(),
            kind: MomentType::NotableEvent,
            confidence: 0.5,
            description: None,
            evidence: Vec::new(),
            provenance: None,
        };
        let v = serde_json::to_value(&p).unwrap();
        assert!(v.get("description").is_none());
        assert!(v.get("evidence").is_none());
        assert!(v.get("provenance").is_none());
    }

    #[test]
    fn into_track_event_has_post_shape() {
        let id = TadsId::new_v4();
        let tr: TimeRange = "[10:0_25:0)".parse().unwrap();
        let ev = sample().into_track_event(id, tr);
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["id"], serde_json::json!(id.to_string()));
        assert_eq!(v["timerange"], serde_json::json!("[10:0_25:0)"));
        assert!(v.get("created").is_none());
        assert_eq!(v["payload"]["title"], serde_json::json!("Goal"));
        assert_eq!(v["payload"]["confidence"], serde_json::json!(0.92));
    }
}
