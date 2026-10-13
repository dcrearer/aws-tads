//! The local mirror of TADS's validation gate: assert the typed payload builders produce JSON that
//! conforms to the committed `vu:*:v1` schemas (vendored under `fixtures/`), plus a deliberately
//! malformed case per type. If these pass but TADS rejects a real POST, the fixtures have drifted
//! from the deployed schemas.

use serde_json::{Value, json};
use tads_domain::{Timestamp, validate_payload};
use vu_domain::{
    AnalysisType, KeyMomentPayload, KeyMomentType, Provenance, TranscriptPayload, TranscriptWord,
    derive_track_id, to_event_post,
};

fn schema(kind: AnalysisType) -> Value {
    let raw = match kind {
        AnalysisType::Transcript => include_str!("fixtures/transcript.schema.json"),
        AnalysisType::KeyMoment => include_str!("fixtures/key-moment.schema.json"),
    };
    serde_json::from_str(raw).expect("fixture is valid JSON")
}

fn value_of(payload: &impl serde::Serialize) -> Value {
    serde_json::to_value(payload).unwrap()
}

#[test]
fn key_moment_builder_conforms() {
    let p = KeyMomentPayload {
        title: "Goal scored".into(),
        kind: KeyMomentType::Highlight,
        description: Some("Striker finishes a counter-attack".into()),
        confidence: 0.92,
        evidence: Some(vec!["crowd roar".into(), "net ripple".into()]),
        provenance: Some(Provenance {
            model: Some("pegasus".into()),
            model_version: Some("1".into()),
            generated_at: Some("2026-10-13T00:00:00Z".into()),
            source: Some("tams://example/flows/proxy".into()),
        }),
    };
    validate_payload(&value_of(&p), &schema(AnalysisType::KeyMoment))
        .expect("key-moment payload should conform");
}

#[test]
fn minimal_key_moment_conforms() {
    // Only the required fields.
    let p = KeyMomentPayload {
        title: "x".into(),
        kind: KeyMomentType::NotableEvent,
        description: None,
        confidence: 0.5,
        evidence: None,
        provenance: None,
    };
    validate_payload(&value_of(&p), &schema(AnalysisType::KeyMoment)).unwrap();
}

#[test]
fn malformed_key_moment_rejected() {
    // confidence out of range, bad enum, and an unexpected field (additionalProperties:false).
    let bad = json!({ "title": "x", "type": "nope", "confidence": 2.0, "extra": 1 });
    assert!(validate_payload(&bad, &schema(AnalysisType::KeyMoment)).is_err());
}

#[test]
fn transcript_builder_conforms() {
    let p = TranscriptPayload {
        text: "Good evening.".into(),
        language: Some("en-US".into()),
        speaker: Some("anchor".into()),
        confidence: Some(0.98),
        words: Some(vec![TranscriptWord {
            text: "Good".into(),
            start: 0.0,
            end: 0.3,
            confidence: Some(0.99),
        }]),
        provenance: None,
    };
    validate_payload(&value_of(&p), &schema(AnalysisType::Transcript)).unwrap();
}

/// The idempotency rationale encoded as a test: a refined timerange must NOT change the Event id
/// (same unit → upsert), while a different unit ordinal must.
#[test]
fn event_id_is_stable_across_timerange_refinement() {
    let track = derive_track_id("source-123", AnalysisType::KeyMoment);
    let anchor = Timestamp::new(1_694_429_247, 0);
    let payload = KeyMomentPayload {
        title: "x".into(),
        kind: KeyMomentType::Highlight,
        description: None,
        confidence: 0.5,
        evidence: None,
        provenance: None,
    };

    let coarse = to_event_post(
        track,
        anchor,
        0,
        "[1694429247:0_1694429257:0)".parse().unwrap(),
        &payload,
    )
    .unwrap();
    let refined = to_event_post(
        track,
        anchor,
        0,
        "[1694429247:500000000_1694429256:0)".parse().unwrap(),
        &payload,
    )
    .unwrap();
    let other_unit = to_event_post(
        track,
        anchor,
        1,
        "[1694429247:0_1694429257:0)".parse().unwrap(),
        &payload,
    )
    .unwrap();

    assert_eq!(
        coarse.id, refined.id,
        "a timerange refinement must upsert the same Event"
    );
    assert_ne!(
        coarse.id, other_unit.id,
        "a different unit must be a different Event"
    );
    assert!(coarse.id.is_some());
}
