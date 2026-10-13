//! Typed Event payloads, one per `vu:*:v1` Payload Schema (see docs/inference-schemas.md).
//!
//! Each struct serializes to the exact JSON the matching schema validates. The schemas use
//! `additionalProperties: false`, so these types carry only known fields and omit absent optionals
//! (`skip_serializing_if`) rather than emitting `null`. Conformance is asserted in
//! `tests/conformance.rs` against the vendored schema fixtures.

use serde::{Deserialize, Serialize};

/// Shared provenance block — which model produced the annotation, and from what.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_version: Option<String>,
    /// RFC 3339 / ISO 8601 timestamp string (`format: date-time`, annotative).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated_at: Option<String>,
    /// e.g. the `tams://` proxy flow analysed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

// ---- vu:transcript:v1 -------------------------------------------------------------------------

/// One transcript utterance/sentence. The Event timerange is the span it covers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TranscriptPayload {
    pub text: String,
    /// BCP-47, e.g. `en-US`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speaker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub words: Option<Vec<TranscriptWord>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Provenance>,
}

/// A word with its relative (seconds-on-the-media-timeline) offsets, as a model emits them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TranscriptWord {
    pub text: String,
    pub start: f64,
    pub end: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
}

// ---- vu:key-moment:v1 -------------------------------------------------------------------------

/// Category of a detected key moment (matches the schema `enum`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyMomentType {
    NotableEvent,
    SpeakerChange,
    Highlight,
    SceneCut,
    TextOnScreen,
}

/// A detected moment of interest. The Event timerange is the moment's span.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyMomentPayload {
    pub title: String,
    #[serde(rename = "type")]
    pub kind: KeyMomentType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub confidence: f64,
    /// Supporting signals/quotes the model cited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Provenance>,
}
