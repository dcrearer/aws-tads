//! Deterministic (UUID v5) identifiers for VU Tracks and Events.
//!
//! Both derivations hash a stable name under a frozen namespace, so the same logical input always
//! yields the same id — which is what makes `PUT /tracks/{id}` and `POST .../events` idempotent on
//! redelivery and reprocessing.

use tads_domain::{TadsId, Timestamp};
use uuid::Uuid;

use crate::AnalysisType;

/// Frozen namespace for all VU deterministic identifiers.
///
/// **Do not change this value.** Every `track_id` and `event_id` is derived from it, so altering it
/// re-keys all VU data and orphans everything already written to TADS.
pub const VU_NAMESPACE: Uuid = Uuid::from_u128(0x7661_755f_6e73_5f74_6164_735f_7675_0001);

/// Derive the Track id for a `(Source, analysis-type)` pair.
///
/// One Track per pair, each bound to that type's schema; deterministic so `PUT /tracks/{id}` is
/// idempotent and the analyzer can address the Track without a lookup.
pub fn derive_track_id(source_id: &str, analysis: AnalysisType) -> TadsId {
    let name = format!("{source_id}:{}", analysis.slug());
    TadsId::from(Uuid::new_v5(&VU_NAMESPACE, name.as_bytes()))
}

/// Derive the Event id for one analysis unit.
///
/// `segment_anchor` is the immutable TAI start of the segment the unit *begins* in; `unit_ordinal`
/// is the model's deterministic index of the unit within that segment. The timerange is
/// deliberately **not** an input, so a refined timerange still maps to the same Event.
pub fn derive_event_id(track_id: TadsId, segment_anchor: Timestamp, unit_ordinal: u32) -> TadsId {
    let name = format!("{track_id}:{segment_anchor}:{unit_ordinal}");
    TadsId::from(Uuid::new_v5(&VU_NAMESPACE, name.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_id_is_stable_and_type_scoped() {
        let a = derive_track_id("source-123", AnalysisType::KeyMoment);
        assert_eq!(a, derive_track_id("source-123", AnalysisType::KeyMoment));
        // Different analysis type on the same Source → a different Track.
        assert_ne!(a, derive_track_id("source-123", AnalysisType::Transcript));
        // Different Source → a different Track.
        assert_ne!(a, derive_track_id("source-999", AnalysisType::KeyMoment));
    }

    #[test]
    fn event_id_ignores_timerange_but_tracks_the_unit() {
        let track = derive_track_id("source-123", AnalysisType::KeyMoment);
        let anchor = Timestamp::new(1_694_429_247, 0);
        let base = derive_event_id(track, anchor, 0);
        // Same (track, anchor, ordinal) → same id (the timerange is not part of it).
        assert_eq!(base, derive_event_id(track, anchor, 0));
        // A different ordinal, anchor, or track → a different id.
        assert_ne!(base, derive_event_id(track, anchor, 1));
        assert_ne!(
            base,
            derive_event_id(track, Timestamp::new(1_694_429_248, 0), 0)
        );
        assert_ne!(
            base,
            derive_event_id(
                derive_track_id("source-123", AnalysisType::Transcript),
                anchor,
                0
            )
        );
    }
}
