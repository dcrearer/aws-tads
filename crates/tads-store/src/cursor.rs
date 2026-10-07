//! Opaque keyset pagination cursors.
//!
//! The cursor is a base64url(JSON) snapshot of the last row's ordering key — e.g.
//! `{"start_ns":123,"event_id":"<uuid>"}` for events, or `{"id":"<uuid>"}` for the id-ordered
//! lists. It is surfaced verbatim as the spec's `page` query param and `X-Paging-NextKey` header;
//! clients never interpret it. Keyset (vs offset) paging keeps each page a single indexed range
//! scan.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;

use crate::error::StoreError;

/// Common list parameters shared by every paginated endpoint.
#[derive(Debug, Clone)]
pub struct PageParams {
    pub limit: Option<i32>,
    pub page: Option<String>,
}

impl PageParams {
    /// Clamp the client limit to a safe server range (default 100, max 1000).
    pub fn effective_limit(&self) -> i32 {
        self.limit.unwrap_or(100).clamp(1, 1000)
    }

    /// Decode the inbound `page` cursor into its JSON keyset object, if present.
    pub fn cursor(&self) -> Result<Option<Value>, StoreError> {
        self.page.as_deref().map(decode_cursor).transpose()
    }
}

/// A page of results plus the cursor for the next page (absent on the last page).
#[derive(Debug, Clone)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_key: Option<String>,
    pub limit: i32,
}

/// Encode a keyset JSON object as an opaque cursor string.
pub fn encode_cursor(key: &Value) -> String {
    URL_SAFE_NO_PAD.encode(key.to_string().as_bytes())
}

/// Decode an opaque cursor string back into its keyset JSON object.
pub fn decode_cursor(cursor: &str) -> Result<Value, StoreError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|e| StoreError::Cursor(e.to_string()))?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|e| StoreError::Cursor(e.to_string()))?;
    if !value.is_object() {
        return Err(StoreError::Cursor("cursor is not a JSON object".into()));
    }
    Ok(value)
}

/// Read a required string field out of a decoded cursor object.
pub fn cursor_str(cursor: &Value, field: &str) -> Result<String, StoreError> {
    cursor
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| StoreError::Cursor(format!("cursor missing `{field}`")))
}

/// Read a required i64 field out of a decoded cursor object.
pub fn cursor_i64(cursor: &Value, field: &str) -> Result<i64, StoreError> {
    cursor
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| StoreError::Cursor(format!("cursor missing `{field}`")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cursor_round_trips() {
        let key = json!({ "start_ns": 12345, "event_id": "abc" });
        let encoded = encode_cursor(&key);
        let decoded = decode_cursor(&encoded).unwrap();
        assert_eq!(cursor_i64(&decoded, "start_ns").unwrap(), 12345);
        assert_eq!(cursor_str(&decoded, "event_id").unwrap(), "abc");
    }

    #[test]
    fn rejects_garbage_cursor() {
        assert!(decode_cursor("!!!not-base64!!!").is_err());
    }

    #[test]
    fn limit_is_clamped() {
        let p = PageParams {
            limit: Some(99999),
            page: None,
        };
        assert_eq!(p.effective_limit(), 1000);
        let p = PageParams {
            limit: None,
            page: None,
        };
        assert_eq!(p.effective_limit(), 100);
    }
}
