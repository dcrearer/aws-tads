//! Deterministic identifier derivation.
//!
//! Every id the pipeline writes is a UUIDv5 under one frozen namespace, so the same logical thing
//! always maps to the same id — making `PUT`/`POST` idempotent and letting re-analysis target
//! exactly the Events it needs to delete. Changing [`VU_NAMESPACE`] would re-key everything, so it
//! is frozen.

use tads_domain::{TadsId, Timestamp};
use uuid::Uuid;

/// Frozen namespace for all VU UUIDv5 derivation. Do not change — it anchors every derived id.
pub const VU_NAMESPACE: Uuid = Uuid::from_u128(0x5f9d_2c7e_8a4b_4e1d_9c3a_1b2c_3d4e_5f60);

fn uuid5(name: &str) -> TadsId {
    Uuid::new_v5(&VU_NAMESPACE, name.as_bytes()).into()
}

/// Deterministic Payload Schema id for a VU external id (e.g. `vu:key-moment:v1`). Lets the writer
/// `PUT /schemas/{id}` and reference it from a Track without a lookup round-trip.
pub fn derive_schema_id(external_id: &str) -> TadsId {
    uuid5(&format!("schema:{external_id}"))
}

/// Deterministic Track id for a (Source, analysis-type) pair. One Track per Source per analysis
/// type; `PUT /tracks/{id}` is therefore itself idempotent.
pub fn derive_track_id(source_uri: &str, analysis_type: &str) -> TadsId {
    uuid5(&format!("track:{source_uri}:{analysis_type}"))
}

/// Deterministic Event id for a moment, keyed on the **immutable** TAI anchor of the segment the
/// moment begins in plus its ordinal within that segment — never on the (window-dependent)
/// timerange. Overlapping look-back windows that re-surface the same moment therefore derive the
/// same id, so a redelivery or re-analysis maps to the same Event rather than a duplicate.
pub fn derive_event_id(track_id: TadsId, segment_anchor: Timestamp, ordinal: u32) -> TadsId {
    uuid5(&format!(
        "event:{track_id}:{}:{ordinal}",
        segment_anchor.as_nanos()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derivation_is_stable() {
        let a = derive_track_id("tams://h/sources/abc", "key-moment");
        let b = derive_track_id("tams://h/sources/abc", "key-moment");
        assert_eq!(a, b);
    }

    #[test]
    fn different_sources_differ() {
        let a = derive_track_id("tams://h/sources/abc", "key-moment");
        let b = derive_track_id("tams://h/sources/def", "key-moment");
        assert_ne!(a, b);
    }

    #[test]
    fn event_id_independent_of_window_but_keyed_on_anchor_and_ordinal() {
        let track = derive_track_id("tams://h/sources/abc", "key-moment");
        let anchor = Timestamp::new(1_700_000_123, 456);
        // Same anchor + ordinal => same id regardless of which window produced it.
        assert_eq!(
            derive_event_id(track, anchor, 0),
            derive_event_id(track, anchor, 0)
        );
        // Different ordinal within the segment => different id.
        assert_ne!(
            derive_event_id(track, anchor, 0),
            derive_event_id(track, anchor, 1)
        );
        // Different anchor => different id.
        assert_ne!(
            derive_event_id(track, anchor, 0),
            derive_event_id(track, Timestamp::new(1_700_000_124, 0), 0)
        );
    }

    #[test]
    fn schema_id_is_stable_and_distinct() {
        assert_eq!(
            derive_schema_id("vu:key-moment:v1"),
            derive_schema_id("vu:key-moment:v1")
        );
        assert_ne!(
            derive_schema_id("vu:key-moment:v1"),
            derive_schema_id("vu:other:v1")
        );
    }
}
