//! Opaque pagination cursors.
//!
//! Maps a DynamoDB `LastEvaluatedKey` to/from the base64 string that the spec surfaces as the
//! `page` query param and `X-Paging-NextKey` header. Only `S`/`N` attribute values appear in
//! our key schemas, so the on-the-wire form is a tiny JSON object the client never interprets.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

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

    /// Decode the inbound `page` cursor into a DynamoDB exclusive-start-key, if present.
    pub fn exclusive_start_key(
        &self,
    ) -> Result<Option<HashMap<String, AttributeValue>>, StoreError> {
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

/// Encode a DynamoDB `LastEvaluatedKey` as an opaque cursor string.
pub fn encode_cursor(key: &HashMap<String, AttributeValue>) -> Result<String, StoreError> {
    let mut map = serde_json::Map::new();
    for (k, v) in key {
        let encoded = match v {
            AttributeValue::S(s) => serde_json::json!({ "S": s }),
            AttributeValue::N(n) => serde_json::json!({ "N": n }),
            other => {
                return Err(StoreError::Cursor(format!(
                    "unsupported key attribute type: {other:?}"
                )));
            }
        };
        map.insert(k.clone(), encoded);
    }
    let bytes = serde_json::to_vec(&serde_json::Value::Object(map))
        .map_err(|e| StoreError::Cursor(e.to_string()))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

/// Decode an opaque cursor string back into a DynamoDB exclusive-start-key.
pub fn decode_cursor(cursor: &str) -> Result<HashMap<String, AttributeValue>, StoreError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|e| StoreError::Cursor(e.to_string()))?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| StoreError::Cursor(e.to_string()))?;
    let obj = value
        .as_object()
        .ok_or_else(|| StoreError::Cursor("cursor is not a JSON object".into()))?;

    let mut key = HashMap::new();
    for (k, v) in obj {
        let av = if let Some(s) = v.get("S").and_then(|x| x.as_str()) {
            AttributeValue::S(s.to_string())
        } else if let Some(n) = v.get("N").and_then(|x| x.as_str()) {
            AttributeValue::N(n.to_string())
        } else {
            return Err(StoreError::Cursor(format!(
                "unsupported cursor attribute `{k}`"
            )));
        };
        key.insert(k.clone(), av);
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips() {
        let mut key = HashMap::new();
        key.insert("track_id".to_string(), AttributeValue::S("abc".to_string()));
        key.insert("sk".to_string(), AttributeValue::N("12345".to_string()));

        let encoded = encode_cursor(&key).unwrap();
        let decoded = decode_cursor(&encoded).unwrap();
        assert_eq!(key, decoded);
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
