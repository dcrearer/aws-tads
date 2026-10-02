//! JSON-Schema validation of Event payloads against a Track's Payload Schema.

use serde_json::Value;

use crate::error::DomainError;

/// Validate `payload` against the JSON Schema `schema`.
///
/// Returns [`DomainError::InvalidSchema`] if `schema` itself is not a usable JSON Schema, or
/// [`DomainError::PayloadValidation`] (with the collected violations) if the payload does not
/// conform. Used by the events handler before persisting a POSTed Event.
pub fn validate_payload(payload: &Value, schema: &Value) -> Result<(), DomainError> {
    let validator =
        jsonschema::validator_for(schema).map_err(|e| DomainError::InvalidSchema(e.to_string()))?;

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

/// Ensure `schema` is itself a usable JSON Schema (compiles). Used when a Payload Schema is
/// created, before it is stored, so Tracks can rely on it later.
pub fn ensure_valid_schema(schema: &Value) -> Result<(), DomainError> {
    jsonschema::validator_for(schema)
        .map(|_| ())
        .map_err(|e| DomainError::InvalidSchema(e.to_string()))
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
}
