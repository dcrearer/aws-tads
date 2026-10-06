//! JSON-Schema validation of Event payloads against a Track's Payload Schema.

use serde_json::Value;

use crate::error::DomainError;

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
