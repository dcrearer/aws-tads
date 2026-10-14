//! Pure domain layer for the video-understanding (VU) pipeline.
//!
//! Deterministic id derivation, the typed key-moment payload + its JSON Schema, and the
//! non-overlapping clip transform. No I/O; reuses [`tads_domain`] types for ids, timeranges, and
//! the outbound Event shape.

pub mod clip;
pub mod error;
pub mod ids;
pub mod payloads;

pub use clip::clip_non_overlapping;
pub use error::VuDomainError;
pub use ids::{VU_NAMESPACE, derive_event_id, derive_schema_id, derive_track_id};
pub use payloads::{
    KEY_MOMENT_ANALYSIS_TYPE, KEY_MOMENT_SCHEMA_EXTERNAL_ID, KEY_MOMENT_SCHEMA_JSON,
    KeyMomentPayload, Moment, MomentType, Provenance,
};
