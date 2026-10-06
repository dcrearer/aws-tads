//! Conformance against real data from a live BBC-TADS v0.1 deployment ("Phrame TADS").
//!
//! Fully offline: loads the exports in `tests/fixtures/` (see that folder's README) and asserts
//! our domain models round-trip production objects and that our JSON-Schema validation path works
//! against the real Payload Schemas. No AWS, no network — pure `tads-domain`.

use std::fs;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tads_domain::{
    PayloadSchema, Track, TrackDeleteRequest, WebhookGet, ensure_valid_schema, validate_payload,
};

fn load<T: DeserializeOwned>(file: &str) -> Vec<T> {
    let path = format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), file);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("deserialize {file}: {e}"))
}

/// serialize∘deserialize must be stable for every item (our model loses nothing it models).
fn assert_round_trips<T>(items: &[T])
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    for item in items {
        let value = serde_json::to_value(item).expect("serialize");
        let back: T = serde_json::from_value(value).expect("deserialize");
        assert_eq!(item, &back, "round-trip changed an object");
    }
}

#[test]
fn schemas_deserialize_and_are_valid_json_schemas() {
    let schemas: Vec<PayloadSchema> = load("schemas.json");
    assert_eq!(schemas.len(), 8, "expected 8 Payload Schemas");
    assert_round_trips(&schemas);

    // Every registered `schema` body must be a usable JSON Schema (draft-07 and 2020-12 both appear).
    for s in &schemas {
        ensure_valid_schema(&s.schema)
            .unwrap_or_else(|e| panic!("schema {:?} not a valid JSON Schema: {e}", s.external_id));
    }
}

#[test]
fn tracks_deserialize_and_reference_schemas_and_timelines() {
    let tracks: Vec<Track> = load("tracks.json");
    assert_eq!(tracks.len(), 94, "expected 94 Tracks");
    assert_round_trips(&tracks);

    let schema_ids: std::collections::HashSet<String> = load::<PayloadSchema>("schemas.json")
        .iter()
        .map(|s| s.id.to_string())
        .collect();

    for t in &tracks {
        assert!(
            !t.timeline_reference.entity_id.is_empty(),
            "track {} has empty timeline_reference.entity_id",
            t.id
        );
        assert!(
            schema_ids.contains(&t.schema_id.to_string()),
            "track {} references unknown schema {}",
            t.id,
            t.schema_id
        );
    }
}

#[test]
fn delete_requests_deserialize_with_timerange_and_ttl() {
    let requests: Vec<TrackDeleteRequest> = load("track-delete-requests.json");
    assert_eq!(requests.len(), 14, "expected 14 delete requests");
    assert_round_trips(&requests);

    for r in &requests {
        // Each targets an event window (delete_track:false) that must parse as a TAI TimeRange —
        // deserialization already proved the parse; assert the window is present.
        assert!(
            r.timerange_to_delete.is_some(),
            "delete request {} has no timerange_to_delete",
            r.id
        );
        // The live instance sets expiry = created + 7 days, matching our DELETE_REQUEST_TTL_DAYS.
        if let (Some(created), Some(expiry)) = (r.created, r.expiry) {
            assert_eq!(
                (expiry - created),
                chrono::Duration::days(7),
                "delete request {} expiry is not created + 7 days",
                r.id
            );
        }
    }
}

#[test]
fn webhooks_export_is_empty() {
    let webhooks: Vec<WebhookGet> = load("webhooks.json");
    assert!(webhooks.is_empty(), "fixture has no registered webhooks");
}

#[test]
fn payloads_validate_against_real_schemas() {
    let schemas: Vec<PayloadSchema> = load("schemas.json");
    let by_ext = |ext: &str| -> Value {
        schemas
            .iter()
            .find(|s| s.external_id.as_deref() == Some(ext))
            .unwrap_or_else(|| panic!("schema {ext} not in fixture"))
            .schema
            .clone()
    };

    // draft-07 schema: subtitle text (required text, pid, dedup_window_seconds).
    let subtitle = by_ext("phrame.compliance-recorder.subtitle-text.v1");
    assert!(
        validate_payload(
            &json!({"text": "ORDER, ORDER", "pid": 5, "dedup_window_seconds": 60}),
            &subtitle
        )
        .is_ok(),
        "conforming subtitle payload should validate"
    );
    assert!(
        validate_payload(&json!({"text": "no pid or window"}), &subtitle).is_err(),
        "payload missing required fields must be rejected"
    );
    assert!(
        validate_payload(
            &json!({"text": "x", "pid": 5, "dedup_window_seconds": 60, "nope": 1}),
            &subtitle
        )
        .is_err(),
        "additionalProperties:false must reject unknown fields"
    );

    // draft-2020-12 schema: wildlife event — proves our validator handles both JSON Schema drafts.
    let wildlife =
        by_ext("https://gitlab.3adesign.co.uk/phrame/metadata-extractor/schemas/wildlife-event-v1");
    assert!(
        validate_payload(
            &json!({
                "schema_version": 1,
                "kind": "motion_event",
                "start_tai_ns": 1,
                "end_tai_ns": 2,
                "source": {"stream": "abc", "substream": "video"}
            }),
            &wildlife,
        )
        .is_ok(),
        "conforming wildlife payload (2020-12) should validate"
    );
    assert!(
        validate_payload(&json!({"kind": "motion_event"}), &wildlife).is_err(),
        "wildlife payload missing required fields must be rejected"
    );
}
