//! Pure request builder and response parser for the TwelveLabs Pegasus InvokeModel contract.
//!
//! Pegasus is invoked via **InvokeModel** (not Converse). We use `analysis_mode:
//! time_based_metadata` with a single `key_moment` segment definition, so the model returns an
//! array of timestamped segments (`start_sec`/`end_sec`) carrying our declared fields. Temperature
//! is pinned to 0 for determinism, which is load-bearing for the pipeline's stable Event ids.

use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::InferError;

/// The segment-definition id Pegasus echoes back on each extracted moment.
pub const KEY_MOMENT_SEGMENT_ID: &str = "key_moment";

/// Inference configuration (model id + output cap). Temperature is always 0.
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    pub model_id: String,
    pub max_output_tokens: i32,
}

/// One raw moment as returned by Pegasus, before TAI mapping / payload typing (that is the worker's
/// job, since provenance and the owning-segment anchor are pipeline context). Field names mirror the
/// TADS `vu:key-moment:v1` schema the model is prompted to produce.
#[derive(Debug, Clone, PartialEq)]
pub struct RawMoment {
    pub start_sec: f64,
    pub end_sec: f64,
    pub title: String,
    /// Free-form `type` label from the model; the worker maps it onto the schema enum.
    pub moment_type: Option<String>,
    pub confidence: Option<f64>,
    pub description: Option<String>,
}

/// Build the InvokeModel request body (base64 media inline) for key-moment extraction. The field
/// definitions mirror the `vu:key-moment:v1` schema so the model emits conforming values.
pub fn build_request(media_base64: &str, cfg: &InferenceConfig) -> Value {
    json!({
        "analysis_mode": "time_based_metadata",
        "mediaSource": { "base64String": media_base64 },
        "temperature": 0,
        "maxOutputTokens": cfg.max_output_tokens,
        "segment_definitions": [
            {
                "id": KEY_MOMENT_SEGMENT_ID,
                "description": "A salient, self-contained moment in the video worth indexing.",
                "fields": [
                    { "name": "title", "type": "string", "description": "Short label for the moment." },
                    { "name": "type", "type": "string", "description": "Exactly one of: notable_event, speaker_change, highlight, scene_cut, text_on_screen." },
                    { "name": "confidence", "type": "number", "description": "Confidence this is a real key moment, in [0, 1]." },
                    { "name": "description", "type": "string", "description": "One or two sentences describing what happens." }
                ]
            }
        ]
    })
}

#[derive(Debug, Deserialize)]
struct RawSegment {
    start_sec: f64,
    end_sec: f64,
    #[serde(default)]
    title: Option<String>,
    #[serde(default, rename = "type")]
    moment_type: Option<String>,
    #[serde(default)]
    confidence: Option<f64>,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TimeBasedMetadataResponse {
    #[serde(default)]
    segments: Vec<RawSegment>,
}

/// Parse a Pegasus `time_based_metadata` response body into raw moments. Segments missing a title
/// are skipped (the model occasionally emits an empty boundary segment) rather than failing the
/// whole batch.
pub fn parse_response(body: &[u8]) -> Result<Vec<RawMoment>, InferError> {
    let parsed: TimeBasedMetadataResponse =
        serde_json::from_slice(body).map_err(|e| InferError::Response(e.to_string()))?;
    let moments = parsed
        .segments
        .into_iter()
        .filter_map(|s| {
            let title = s.title.filter(|t| !t.is_empty())?;
            Some(RawMoment {
                start_sec: s.start_sec,
                end_sec: s.end_sec,
                title,
                moment_type: s.moment_type,
                confidence: s.confidence,
                description: s.description,
            })
        })
        .collect();
    Ok(moments)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> InferenceConfig {
        InferenceConfig {
            model_id: "us.twelvelabs.pegasus-1-5-v1:0".into(),
            max_output_tokens: 4096,
        }
    }

    #[test]
    fn request_body_has_expected_shape() {
        let v = build_request("Zm9vYmFy", &cfg());
        assert_eq!(v["analysis_mode"], json!("time_based_metadata"));
        assert_eq!(v["temperature"], json!(0));
        assert_eq!(v["maxOutputTokens"], json!(4096));
        assert_eq!(v["mediaSource"]["base64String"], json!("Zm9vYmFy"));
        assert_eq!(
            v["segment_definitions"][0]["id"],
            json!(KEY_MOMENT_SEGMENT_ID)
        );
        let field_names: Vec<&str> = v["segment_definitions"][0]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        assert!(field_names.contains(&"title"));
        assert!(field_names.contains(&"type"));
        assert!(field_names.contains(&"confidence"));
    }

    #[test]
    fn parses_segments_and_skips_titleless() {
        let body = br#"{
            "segments": [
                { "id": "key_moment", "start_sec": 0, "end_sec": 12.5, "title": "Open", "type": "scene_cut", "confidence": 0.3, "description": "Establishing shot." },
                { "id": "key_moment", "start_sec": 12.5, "end_sec": 30, "title": "Goal", "type": "highlight" },
                { "id": "key_moment", "start_sec": 30, "end_sec": 31, "description": "no title here" }
            ],
            "definition_errors": [],
            "stopReason": "stop",
            "finish_reason": "stop"
        }"#;
        let moments = parse_response(body).unwrap();
        assert_eq!(moments.len(), 2);
        assert_eq!(moments[0].title, "Open");
        assert_eq!(moments[0].end_sec, 12.5);
        assert_eq!(moments[0].moment_type.as_deref(), Some("scene_cut"));
        assert_eq!(moments[0].confidence, Some(0.3));
        assert_eq!(moments[1].moment_type.as_deref(), Some("highlight"));
        assert_eq!(moments[1].confidence, None);
    }

    #[test]
    fn empty_segments_is_ok() {
        let body = br#"{ "segments": [], "stopReason": "stop" }"#;
        assert!(parse_response(body).unwrap().is_empty());
    }

    #[test]
    fn malformed_body_errors() {
        assert!(parse_response(b"not json").is_err());
    }
}
