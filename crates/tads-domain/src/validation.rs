//! JSON-Schema validation of Event payloads against a Track's Payload Schema.

use serde_json::Value;

use crate::error::DomainError;
use crate::models::TimelineReference;

/// A compiled JSON Schema validator, reusable across many payloads.
///
/// Compiling a schema is ~150× more expensive than validating one payload against an
/// already-compiled validator, so batch paths (e.g. a multi-Event POST, all against one Track's
/// schema) should [`compile_schema`] once and [`validate_with`] per payload rather than calling
/// [`validate_payload`] in a loop.
pub type PayloadValidator = jsonschema::Validator;

/// Compile a JSON Schema into a reusable [`PayloadValidator`].
pub fn compile_schema(schema: &Value) -> Result<PayloadValidator, DomainError> {
    jsonschema::validator_for(schema).map_err(|e| DomainError::InvalidSchema(e.to_string()))
}

/// Validate `payload` against an already-compiled validator, collecting any violations.
pub fn validate_with(validator: &PayloadValidator, payload: &Value) -> Result<(), DomainError> {
    let errors: Vec<String> = validator
        .iter_errors(payload)
        .map(|e| e.to_string())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(DomainError::PayloadValidation(errors.join("; ")))
    }
}

/// Validate `payload` against the JSON Schema `schema`, compiling the schema on the fly.
///
/// Convenience for one-off validation. In a loop over many payloads sharing one schema, prefer
/// [`compile_schema`] + [`validate_with`] to avoid recompiling per payload.
pub fn validate_payload(payload: &Value, schema: &Value) -> Result<(), DomainError> {
    validate_with(&compile_schema(schema)?, payload)
}

/// Ensure `schema` is itself a usable JSON Schema (compiles). Used when a Payload Schema is
/// created, before it is stored, so Tracks can rely on it later.
pub fn ensure_valid_schema(schema: &Value) -> Result<(), DomainError> {
    jsonschema::validator_for(schema)
        .map(|_| ())
        .map_err(|e| DomainError::InvalidSchema(e.to_string()))
}

/// Enforce TADS AppNote 0001's rule that a TAMS (`tams://`) `timeline_reference` MUST NOT point at
/// a specific TimeRange of a Source or Flow (the TAMS AppNote 0014 TimeRange URI variants). A
/// Track's timing comes from its Events, so the reference identifies the whole Source/Flow timeline.
///
/// Scope is deliberately limited to the `tams://` pseudo-protocol: TADS also annotates non-TAMS,
/// file-based media whose `entity_id` may be an arbitrary URI that legitimately carries a query
/// string. We detect the variant by a `timerange` query parameter (covers both the Source form
/// `.../sources/{id}?timerange=...` and the Flow form `.../flows/{id}/segments?timerange=...`).
pub fn validate_timeline_reference(tr: &TimelineReference) -> Result<(), DomainError> {
    let id = tr.entity_id.trim();
    let is_tams = id.len() >= 7 && id.as_bytes()[..7].eq_ignore_ascii_case(b"tams://");
    if is_tams && let Some((_, query)) = id.split_once('?') {
        let has_timerange = query.split('&').any(|param| {
            param
                .split('=')
                .next()
                .is_some_and(|key| key.eq_ignore_ascii_case("timerange"))
        });
        if has_timerange {
            return Err(DomainError::InvalidTimelineReference(format!(
                "`{}`: a tams:// reference MUST NOT point at a specific TimeRange of a Source/Flow \
                 (TADS AppNote 0001); reference the whole Source/Flow instead",
                tr.entity_id
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_conforming_payload() {
        let schema = json!({
            "type": "object",
            "required": ["text"],
            "properties": { "text": { "type": "string" } }
        });
        assert!(validate_payload(&json!({ "text": "hello" }), &schema).is_ok());
    }

    #[test]
    fn rejects_nonconforming_payload() {
        let schema = json!({
            "type": "object",
            "required": ["text"],
            "properties": { "text": { "type": "string" } }
        });
        let err = validate_payload(&json!({ "text": 42 }), &schema).unwrap_err();
        assert!(matches!(err, DomainError::PayloadValidation(_)));
    }

    #[test]
    fn rejects_invalid_schema() {
        // `type` must be a string/array, not a number.
        let bad_schema = json!({ "type": 123 });
        let err = validate_payload(&json!({}), &bad_schema).unwrap_err();
        assert!(matches!(err, DomainError::InvalidSchema(_)));
    }

    use crate::models::TimelineReference;

    fn tref(entity_id: &str) -> TimelineReference {
        TimelineReference {
            entity_id: entity_id.to_string(),
            timeline_selected: None,
        }
    }

    // TADS AppNote 0001: the TAMS (AppNote 0014) URI variants referring to a specific TimeRange of
    // a Source or Flow MUST NOT be used as a `tams://` timeline_reference — the Track's Events
    // carry timing. We reject them with a 400 at write time.
    #[test]
    fn rejects_tams_source_timerange_variant() {
        let err = validate_timeline_reference(&tref(
            "tams://tams.example.com/sources/7abe4d59-7d1c-432d-ba2a-6180e12137cc?timerange=[100:0_200:0)",
        ))
        .unwrap_err();
        assert!(matches!(err, DomainError::InvalidTimelineReference(_)));
    }

    #[test]
    fn rejects_tams_flow_segments_timerange_variant() {
        let err = validate_timeline_reference(&tref(
            "tams://tams.example.com/flows/f87f7f32-a34d-4a6f-b085-a5ee49b4b2f8/segments?timerange=[100:0_200:0)",
        ))
        .unwrap_err();
        assert!(matches!(err, DomainError::InvalidTimelineReference(_)));
    }

    #[test]
    fn rejects_timerange_variant_case_insensitive_scheme() {
        let err = validate_timeline_reference(&tref(
            "TAMS://tams.example.com/sources/7abe4d59-7d1c-432d-ba2a-6180e12137cc?timerange=[0:0_1:0)",
        ))
        .unwrap_err();
        assert!(matches!(err, DomainError::InvalidTimelineReference(_)));
    }

    #[test]
    fn allows_whole_tams_source_reference() {
        assert!(
            validate_timeline_reference(&tref(
                "tams://tams.example.com/sources/7abe4d59-7d1c-432d-ba2a-6180e12137cc"
            ))
            .is_ok()
        );
    }

    #[test]
    fn allows_whole_tams_flow_reference() {
        assert!(
            validate_timeline_reference(&tref(
                "tams://tams.example.com/flows/f87f7f32-a34d-4a6f-b085-a5ee49b4b2f8"
            ))
            .is_ok()
        );
    }

    // Only `tams://` references are in scope; a non-TAMS URI may legitimately carry a query string
    // (including a `timerange` param of its own semantics), so we must not reject it.
    #[test]
    fn allows_non_tams_uri_with_timerange_query() {
        assert!(
            validate_timeline_reference(&tref(
                "https://other.example.com/thing?timerange=[0:0_1:0)"
            ))
            .is_ok()
        );
    }

    // A `tams://` reference with some other (non-timerange) query is not a TimeRange variant.
    #[test]
    fn allows_tams_reference_with_unrelated_query() {
        assert!(
            validate_timeline_reference(&tref(
                "tams://tams.example.com/sources/7abe4d59-7d1c-432d-ba2a-6180e12137cc?foo=bar"
            ))
            .is_ok()
        );
    }

    // Non-URI opaque ids (TADS also annotates non-TAMS, file-based media) pass through.
    #[test]
    fn allows_opaque_non_uri_entity_id() {
        assert!(validate_timeline_reference(&tref("my-local-asset-42")).is_ok());
    }
}
