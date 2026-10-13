//! Bedrock inference for the key-moment analysis: the **prompt contract**, the structured-output
//! tool schema, the invoker (Converse API, forced tool-use), and the mapping of the model's output
//! into typed [`vu_domain`] payloads with relative timing for `vu-time` to make absolute.
//!
//! The pipeline is deterministic (one structured-output call per segment, temperature 0). That
//! determinism is load-bearing: the `unit_ordinal` in each event id assumes the model returns the
//! same ordered list for the same input, so re-analysis upserts rather than churns.
//!
//! Prompt building, the tool schema, response parsing, and the Document⇄JSON bridge are pure and
//! unit-tested; the live Converse call is exercised by opt-in/mocked tests.

use aws_sdk_bedrockruntime::types::{
    ContentBlock, ConversationRole, ImageBlock, ImageFormat, ImageSource, InferenceConfiguration,
    Message, SpecificToolChoice, Tool, ToolChoice, ToolConfiguration, ToolInputSchema,
    ToolSpecification,
};
use aws_smithy_types::{Blob, Document, Number};
use serde::Deserialize;
use serde_json::{Value, json};
use vu_domain::{KeyMomentPayload, KeyMomentType, Provenance};

/// The tool the model must call to return key moments.
pub const KEY_MOMENT_TOOL: &str = "record_key_moments";

/// Temperature for all inference calls — zero for deterministic, reproducible unit ordering.
const TEMPERATURE: f32 = 0.0;

#[derive(Debug, thiserror::Error)]
pub enum VuInferError {
    #[error("bedrock converse failed: {0}")]
    Bedrock(String),
    #[error("building the bedrock request failed: {0}")]
    Build(String),
    #[error("bedrock returned no usable output")]
    NoOutput,
    #[error("model did not call the `{0}` tool")]
    NoToolUse(&'static str),
    #[error("could not parse model output: {0}")]
    Parse(String),
}

/// Context handed to the prompt. `prior_summary` is the optional soft look-back (a short recap of
/// the previous segment) — cheap continuity for moments that build across a boundary.
#[derive(Debug, Clone, Default)]
pub struct AnalysisContext {
    pub segment_duration_secs: f64,
    pub prior_summary: Option<String>,
}

/// One detected moment: relative timing (seconds within the analysed clip, for `vu-time` to make
/// absolute) plus the typed payload.
#[derive(Debug, Clone, PartialEq)]
pub struct DetectedUnit {
    pub rel_start: f64,
    pub rel_end: f64,
    pub payload: KeyMomentPayload,
}

/// Build the key-moment prompt. Encodes the task, the exact category vocabulary (which must match
/// the `vu:key-moment:v1` enum), the timing convention, and the tool-call instruction.
pub fn key_moment_prompt(ctx: &AnalysisContext) -> String {
    let mut p = String::new();
    p.push_str(
        "You are a video-understanding analyst. You are shown sampled frames from ONE short video \
         clip, in order. Identify the KEY MOMENTS in this clip — distinct, noteworthy things a \
         viewer would want to jump to.\n\n",
    );
    p.push_str("Categorise each moment with exactly one of these types:\n");
    p.push_str(
        "  - notable_event: a significant occurrence in the content\n\
         \x20 - speaker_change: the speaking/presenting person changes\n\
         \x20 - highlight: a peak or standout moment\n\
         \x20 - scene_cut: a clear change of scene or setting\n\
         \x20 - text_on_screen: salient on-screen text/graphics appears\n\n",
    );
    p.push_str(&format!(
        "Timing: express `start` and `end` in SECONDS measured from the start of THIS clip \
         (0.0 to {:.3}). Do not use wall-clock or absolute time.\n",
        ctx.segment_duration_secs
    ));
    p.push_str(
        "Give each moment a short `title`, a `confidence` in [0,1], and optionally a one-line \
         `description` and `evidence` (what in the frames supports it). Do not invent moments; if \
         nothing noteworthy occurs, return an empty list. Do not report the same moment twice.\n",
    );
    if let Some(summary) = &ctx.prior_summary {
        p.push_str(&format!(
            "\nContext from the immediately preceding clip (for continuity only — do NOT re-report \
             its moments): {summary}\n"
        ));
    }
    p.push_str(&format!(
        "\nReturn your answer by calling the `{KEY_MOMENT_TOOL}` tool.",
    ));
    p
}

/// JSON Schema for the tool input — the structured-output contract.
pub fn key_moment_tool_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "moments": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "start": { "type": "number", "description": "seconds from the start of this clip" },
                        "end": { "type": "number", "description": "seconds from the start of this clip" },
                        "title": { "type": "string" },
                        "type": {
                            "type": "string",
                            "enum": ["notable_event", "speaker_change", "highlight", "scene_cut", "text_on_screen"]
                        },
                        "description": { "type": "string" },
                        "confidence": { "type": "number", "minimum": 0, "maximum": 1 },
                        "evidence": { "type": "array", "items": { "type": "string" } }
                    },
                    "required": ["start", "end", "title", "type", "confidence"]
                }
            }
        },
        "required": ["moments"]
    })
}

#[derive(Deserialize)]
struct RawResponse {
    #[serde(default)]
    moments: Vec<RawMoment>,
}

#[derive(Deserialize)]
struct RawMoment {
    start: f64,
    end: f64,
    title: String,
    #[serde(rename = "type")]
    kind: KeyMomentType,
    #[serde(default)]
    description: Option<String>,
    confidence: f64,
    #[serde(default)]
    evidence: Option<Vec<String>>,
}

/// Map the model's tool input into typed [`DetectedUnit`]s, stamping each with `provenance`.
pub fn parse_key_moments(
    tool_input: &Value,
    provenance: &Provenance,
) -> Result<Vec<DetectedUnit>, VuInferError> {
    let raw: RawResponse = serde_json::from_value(tool_input.clone())
        .map_err(|e| VuInferError::Parse(e.to_string()))?;
    Ok(raw
        .moments
        .into_iter()
        .map(|m| DetectedUnit {
            rel_start: m.start,
            rel_end: m.end,
            payload: KeyMomentPayload {
                title: m.title,
                kind: m.kind,
                description: m.description,
                confidence: m.confidence,
                evidence: m.evidence,
                provenance: Some(provenance.clone()),
            },
        })
        .collect())
}

/// Bedrock-backed key-moment analyzer.
#[derive(Clone)]
pub struct KeyMomentAnalyzer {
    client: aws_sdk_bedrockruntime::Client,
    model_id: String,
}

impl KeyMomentAnalyzer {
    pub fn new(client: aws_sdk_bedrockruntime::Client, model_id: impl Into<String>) -> Self {
        Self {
            client,
            model_id: model_id.into(),
        }
    }

    /// Analyse a segment's sampled JPEG frames, returning detected moments with relative timing.
    #[tracing::instrument(skip(self, frames_jpeg, ctx, provenance), fields(model_id = %self.model_id, frames = frames_jpeg.len()))]
    pub async fn analyze(
        &self,
        frames_jpeg: &[Vec<u8>],
        ctx: &AnalysisContext,
        provenance: Provenance,
    ) -> Result<Vec<DetectedUnit>, VuInferError> {
        let mut message = Message::builder()
            .role(ConversationRole::User)
            .content(ContentBlock::Text(key_moment_prompt(ctx)));
        for jpeg in frames_jpeg {
            let image = ImageBlock::builder()
                .format(ImageFormat::Jpeg)
                .source(ImageSource::Bytes(Blob::new(jpeg.clone())))
                .build()
                .map_err(|e| VuInferError::Build(e.to_string()))?;
            message = message.content(ContentBlock::Image(image));
        }
        let message = message
            .build()
            .map_err(|e| VuInferError::Build(e.to_string()))?;

        let tool = Tool::ToolSpec(
            ToolSpecification::builder()
                .name(KEY_MOMENT_TOOL)
                .description("Record the key moments found in the clip.")
                .input_schema(ToolInputSchema::Json(json_to_document(
                    &key_moment_tool_schema(),
                )))
                .build()
                .map_err(|e| VuInferError::Build(e.to_string()))?,
        );
        let tool_config = ToolConfiguration::builder()
            .tools(tool)
            .tool_choice(ToolChoice::Tool(
                SpecificToolChoice::builder()
                    .name(KEY_MOMENT_TOOL)
                    .build()
                    .map_err(|e| VuInferError::Build(e.to_string()))?,
            ))
            .build()
            .map_err(|e| VuInferError::Build(e.to_string()))?;

        let resp = self
            .client
            .converse()
            .model_id(&self.model_id)
            .messages(message)
            .tool_config(tool_config)
            .inference_config(
                InferenceConfiguration::builder()
                    .temperature(TEMPERATURE)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| VuInferError::Bedrock(e.to_string()))?;

        let out = resp.output().ok_or(VuInferError::NoOutput)?;
        let msg = out.as_message().map_err(|_| VuInferError::NoOutput)?;
        for block in msg.content() {
            if let Ok(tu) = block.as_tool_use()
                && tu.name() == KEY_MOMENT_TOOL
            {
                let value = document_to_json(tu.input());
                return parse_key_moments(&value, &provenance);
            }
        }
        Err(VuInferError::NoToolUse(KEY_MOMENT_TOOL))
    }
}

// ---- Document <-> serde_json bridge (Bedrock tool I/O uses smithy Documents) ------------------

fn json_to_document(v: &Value) -> Document {
    match v {
        Value::Null => Document::Null,
        Value::Bool(b) => Document::Bool(*b),
        Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                Document::Number(Number::PosInt(u))
            } else if let Some(i) = n.as_i64() {
                Document::Number(Number::NegInt(i))
            } else {
                Document::Number(Number::Float(n.as_f64().unwrap_or(0.0)))
            }
        }
        Value::String(s) => Document::String(s.clone()),
        Value::Array(a) => Document::Array(a.iter().map(json_to_document).collect()),
        Value::Object(o) => Document::Object(
            o.iter()
                .map(|(k, v)| (k.clone(), json_to_document(v)))
                .collect(),
        ),
    }
}

fn document_to_json(d: &Document) -> Value {
    match d {
        Document::Null => Value::Null,
        Document::Bool(b) => Value::Bool(*b),
        Document::Number(Number::PosInt(u)) => Value::from(*u),
        Document::Number(Number::NegInt(i)) => Value::from(*i),
        Document::Number(Number::Float(f)) => Value::from(*f),
        Document::String(s) => Value::String(s.clone()),
        Document::Array(a) => Value::Array(a.iter().map(document_to_json).collect()),
        Document::Object(o) => Value::Object(
            o.iter()
                .map(|(k, v)| (k.clone(), document_to_json(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> AnalysisContext {
        AnalysisContext {
            segment_duration_secs: 6.0,
            prior_summary: None,
        }
    }

    #[test]
    fn prompt_lists_every_category_and_duration() {
        let p = key_moment_prompt(&ctx());
        for cat in [
            "notable_event",
            "speaker_change",
            "highlight",
            "scene_cut",
            "text_on_screen",
        ] {
            assert!(p.contains(cat), "prompt missing category {cat}");
        }
        assert!(p.contains("6.000"));
        assert!(p.contains(KEY_MOMENT_TOOL));
    }

    #[test]
    fn prompt_includes_prior_summary_when_present() {
        let c = AnalysisContext {
            segment_duration_secs: 4.0,
            prior_summary: Some("a goal was building up".into()),
        };
        assert!(key_moment_prompt(&c).contains("a goal was building up"));
        assert!(!key_moment_prompt(&ctx()).contains("preceding clip"));
    }

    #[test]
    fn parses_tool_output_into_units_with_provenance() {
        let prov = Provenance {
            model: Some("pegasus".into()),
            model_version: Some("1".into()),
            generated_at: None,
            source: None,
        };
        let input = json!({
            "moments": [
                { "start": 1.0, "end": 3.5, "title": "Kickoff", "type": "highlight", "confidence": 0.9 },
                { "start": 4.0, "end": 4.0, "title": "Caption", "type": "text_on_screen",
                  "confidence": 0.7, "description": "lower-third", "evidence": ["white text"] }
            ]
        });
        let units = parse_key_moments(&input, &prov).unwrap();
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].rel_start, 1.0);
        assert_eq!(units[0].rel_end, 3.5);
        assert_eq!(units[0].payload.kind, KeyMomentType::Highlight);
        assert_eq!(
            units[0]
                .payload
                .provenance
                .as_ref()
                .unwrap()
                .model
                .as_deref(),
            Some("pegasus")
        );
        assert_eq!(
            units[1].payload.evidence.as_deref(),
            Some(&["white text".to_string()][..])
        );
    }

    #[test]
    fn empty_moments_is_ok() {
        let units = parse_key_moments(&json!({ "moments": [] }), &Provenance::default()).unwrap();
        assert!(units.is_empty());
    }

    #[test]
    fn bad_category_is_rejected() {
        let bad = json!({ "moments": [{ "start": 0, "end": 1, "title": "x", "type": "nope", "confidence": 0.5 }] });
        assert!(parse_key_moments(&bad, &Provenance::default()).is_err());
    }

    #[test]
    fn document_json_round_trips() {
        let v = key_moment_tool_schema();
        assert_eq!(document_to_json(&json_to_document(&v)), v);
    }
}
