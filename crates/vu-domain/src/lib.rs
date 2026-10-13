//! Pure domain layer for the video-understanding write path: the analysis vocabulary, the typed
//! Event payloads (one per `vu:*:v1` Payload Schema), and the deterministic identifiers that make
//! writes idempotent.
//!
//! This crate maps model output into the shape TADS validates and POSTs it; it does **not**
//! re-validate at runtime (TADS owns the gate). The committed `vu:*:v1` schemas are vendored into
//! this crate's test fixtures so the builders are asserted to conform in CI — the local mirror of
//! TADS's gate. No I/O; fully unit-testable.

pub mod ids;
pub mod payloads;

pub use ids::{VU_NAMESPACE, derive_event_id, derive_track_id};
pub use payloads::{
    KeyMomentPayload, KeyMomentType, Provenance, TranscriptPayload, TranscriptWord,
};

use serde::Serialize;
use tads_domain::{TadsId, TimeRange, Timestamp, TrackEventPost};

/// The three kinds of analysis this pipeline produces. Each maps 1:1 to a TADS Payload Schema
/// (resolved by [`AnalysisType::external_id`]) and to its own Track (so a Track validates against
/// exactly one schema — see docs/inference-plan.md §4.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AnalysisType {
    Transcript,
    KeyMoment,
}

impl AnalysisType {
    /// Stable slug used in `track_id` derivation and tags (`transcript`, `key-moment`).
    pub fn slug(self) -> &'static str {
        match self {
            AnalysisType::Transcript => "transcript",
            AnalysisType::KeyMoment => "key-moment",
        }
    }

    /// The TADS Payload Schema `external_id` this analysis validates against.
    pub fn external_id(self) -> &'static str {
        match self {
            AnalysisType::Transcript => "vu:transcript:v1",
            AnalysisType::KeyMoment => "vu:key-moment:v1",
        }
    }
}

/// Errors building a Track Event from model output.
#[derive(Debug, thiserror::Error)]
pub enum VuDomainError {
    #[error("failed to serialize VU payload: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Build an idempotent [`TrackEventPost`] from a typed payload.
///
/// The `id` is derived from `(track_id, segment_anchor, unit_ordinal)` — **not** the timerange — so
/// re-running analysis on the same media unit (even if a later pass refines the `timerange`) targets
/// the same Event and TADS upserts it, rather than creating a duplicate (docs/inference-plan.md §4.4).
pub fn to_event_post(
    track_id: TadsId,
    segment_anchor: Timestamp,
    unit_ordinal: u32,
    timerange: TimeRange,
    payload: &impl Serialize,
) -> Result<TrackEventPost, VuDomainError> {
    Ok(TrackEventPost {
        id: Some(derive_event_id(track_id, segment_anchor, unit_ordinal)),
        timerange,
        payload: serde_json::to_value(payload)?,
    })
}
