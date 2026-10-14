//! The vendored `vu:key-moment:v1` schema is a real JSON Schema, and typed payloads validate
//! against it — the local mirror of the server-side TADS validation gate.

use serde_json::json;
use tads_domain::{compile_schema, validate_with};
use vu_domain::KEY_MOMENT_SCHEMA_JSON;
use vu_domain::payloads::{KeyMomentPayload, MomentType, Provenance};

fn schema() -> serde_json::Value {
    serde_json::from_str(KEY_MOMENT_SCHEMA_JSON).expect("schema is valid JSON")
}

#[test]
fn key_moment_schema_compiles() {
    assert!(compile_schema(&schema()).is_ok());
}

#[test]
fn typed_payload_conforms() {
    let validator = compile_schema(&schema()).unwrap();
    let payload = KeyMomentPayload {
        title: "Kickoff".into(),
        kind: MomentType::NotableEvent,
        confidence: 0.4,
        description: Some("Match begins at the centre circle.".into()),
        evidence: vec!["whistle".into()],
        provenance: Some(Provenance {
            model: Some("us.twelvelabs.pegasus-1-5-v1:0".into()),
            model_version: None,
            source: Some("tams://host/sources/abc".into()),
            generated_at: Some("2026-10-13T23:00:00Z".into()),
        }),
    };
    let value = serde_json::to_value(&payload).unwrap();
    validate_with(&validator, &value).expect("typed payload must conform");
}

#[test]
fn every_moment_type_conforms() {
    let validator = compile_schema(&schema()).unwrap();
    for kind in [
        MomentType::NotableEvent,
        MomentType::SpeakerChange,
        MomentType::Highlight,
        MomentType::SceneCut,
        MomentType::TextOnScreen,
    ] {
        let value = serde_json::to_value(KeyMomentPayload {
            title: "x".into(),
            kind,
            confidence: 0.5,
            description: None,
            evidence: Vec::new(),
            provenance: None,
        })
        .unwrap();
        validate_with(&validator, &value).expect("each enum value must conform");
    }
}

#[test]
fn minimal_payload_conforms() {
    let validator = compile_schema(&schema()).unwrap();
    let value = json!({ "title": "x", "type": "highlight", "confidence": 0.9 });
    validate_with(&validator, &value).expect("minimal payload must conform");
}

#[test]
fn missing_required_field_is_rejected() {
    let validator = compile_schema(&schema()).unwrap();
    // Missing `confidence`.
    let value = json!({ "title": "x", "type": "highlight" });
    assert!(
        validate_with(&validator, &value).is_err(),
        "missing confidence must fail"
    );
}

#[test]
fn additional_properties_are_rejected() {
    let validator = compile_schema(&schema()).unwrap();
    let value = json!({
        "title": "x", "type": "highlight", "confidence": 0.5, "unexpected": true
    });
    assert!(
        validate_with(&validator, &value).is_err(),
        "extra property must fail"
    );
}

#[test]
fn out_of_range_confidence_is_rejected() {
    let validator = compile_schema(&schema()).unwrap();
    let value = json!({ "title": "x", "type": "highlight", "confidence": 1.5 });
    assert!(
        validate_with(&validator, &value).is_err(),
        "confidence > 1 must fail"
    );
}

#[test]
fn bad_type_enum_is_rejected() {
    let validator = compile_schema(&schema()).unwrap();
    let value = json!({ "title": "x", "type": "presentation", "confidence": 0.5 });
    assert!(
        validate_with(&validator, &value).is_err(),
        "unknown type must fail"
    );
}
