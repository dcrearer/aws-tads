//! Webhook receiver: the TAMS callback ingress (docs/inference-plan.md §2a).
//!
//! Verifies the inbound shared secret, then dispatches:
//! - `flows/created` → **Phase A**: ensure a TADS Track per analysis type for the new Source.
//! - `flows/segments_added` → **Phase B**: enqueue the event onto the SQS work queue for the analyzer.
//!
//! Classification and Track construction are pure and unit-tested; the HTTP/AWS wiring is in main.rs.

use serde_json::Value;
use tads_domain::{TagValue, Tags, TimelineReference, Track};
use vu_domain::{AnalysisType, derive_track_id};

/// Analysis types a new Flow gets Tracks for (Phase A). Key-moment is wired end-to-end today;
/// transcript joins when its analyzer path lands. (Routing refinement is §13.)
pub const ENABLED_TYPES: &[AnalysisType] = &[AnalysisType::KeyMoment];

/// What an inbound TAMS callback asks us to do.
#[derive(Debug, PartialEq)]
pub enum Incoming {
    /// Phase A — ensure Tracks for a newly-created Flow's Source.
    FlowCreated { source_id: String },
    /// Phase B — enqueue this `segments_added` event for the analyzer.
    SegmentsAdded { event: Value },
    /// A callback we acknowledge but don't act on.
    Ignored(String),
}

/// Classify a TAMS webhook callback body. TAMS wraps the payload under `event` with an `event_type`
/// discriminator; we tolerate the payload fields appearing at the top level too.
pub fn parse_incoming(body: &Value) -> Incoming {
    let event_type = body.get("event_type").and_then(Value::as_str).unwrap_or("");
    let event = body.get("event").unwrap_or(body);
    match event_type {
        "flows/created" => match extract_source_id(event) {
            Some(source_id) => Incoming::FlowCreated { source_id },
            None => Incoming::Ignored("flows/created without a source_id".into()),
        },
        "flows/segments_added" => Incoming::SegmentsAdded {
            event: event.clone(),
        },
        other => Incoming::Ignored(other.to_string()),
    }
}

fn extract_source_id(event: &Value) -> Option<String> {
    event
        .get("source_id")
        .and_then(Value::as_str)
        .or_else(|| {
            event
                .get("flow")
                .and_then(|f| f.get("source_id"))
                .and_then(Value::as_str)
        })
        .map(str::to_string)
}

/// Build the Track to `PUT` for one `(Source, analysis-type)`: deterministic id, whole-Source
/// `timeline_reference`, overlaps allowed (docs/inference-plan.md §4.5).
pub fn build_track(
    source_id: &str,
    analysis: AnalysisType,
    schema_id: tads_domain::TadsId,
    entity_base: &str,
) -> Track {
    let mut tags = Tags::default();
    tags.0.insert(
        "producer".into(),
        TagValue::One("video-understanding".into()),
    );
    tags.0
        .insert("analysis".into(), TagValue::One(analysis.slug().into()));
    Track {
        id: derive_track_id(source_id, analysis),
        label: Some(format!("VU {} — {source_id}", analysis.slug())),
        description: None,
        created_by: None,
        updated_by: None,
        timeline_reference: TimelineReference {
            entity_id: format!("{}/sources/{source_id}", entity_base.trim_end_matches('/')),
            timeline_selected: None,
        },
        tags,
        metadata_version: None,
        created: None,
        metadata_updated: None,
        events_updated: None,
        read_only: false,
        allow_overlaps: true,
        editable_events: true,
        timerange: None,
        schema_id,
        edit_rate: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classifies_flow_created() {
        let body = json!({ "event_type": "flows/created", "event": { "source_id": "s1" } });
        assert_eq!(
            parse_incoming(&body),
            Incoming::FlowCreated {
                source_id: "s1".into()
            }
        );
    }

    #[test]
    fn classifies_flow_created_nested_source() {
        let body =
            json!({ "event_type": "flows/created", "event": { "flow": { "source_id": "s2" } } });
        assert_eq!(
            parse_incoming(&body),
            Incoming::FlowCreated {
                source_id: "s2".into()
            }
        );
    }

    #[test]
    fn classifies_segments_added_and_unwraps_event() {
        let body = json!({
            "event_type": "flows/segments_added",
            "event": { "flow_id": "f1", "source_id": "s1", "segments": [] }
        });
        match parse_incoming(&body) {
            Incoming::SegmentsAdded { event } => {
                assert_eq!(event.get("flow_id").unwrap(), "f1");
            }
            other => panic!("expected SegmentsAdded, got {other:?}"),
        }
    }

    #[test]
    fn ignores_other_event_types() {
        let body = json!({ "event_type": "flows/deleted", "event": {} });
        assert_eq!(
            parse_incoming(&body),
            Incoming::Ignored("flows/deleted".into())
        );
    }

    #[test]
    fn builds_whole_source_track() {
        let schema_id = "d4ba1f7d-3d88-4c19-b693-1e701aa853d8".parse().unwrap();
        let t = build_track(
            "s1",
            AnalysisType::KeyMoment,
            schema_id,
            "tams://tams.example.com/",
        );
        assert_eq!(t.id, derive_track_id("s1", AnalysisType::KeyMoment));
        assert_eq!(
            t.timeline_reference.entity_id,
            "tams://tams.example.com/sources/s1"
        );
        assert!(t.allow_overlaps);
        assert_eq!(t.schema_id, schema_id);
        assert!(t.tags.matches("analysis", "key-moment"));
    }
}
