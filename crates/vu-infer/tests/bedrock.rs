//! End-to-end parse of a Pegasus `time_based_metadata` response via a mocked Bedrock InvokeModel
//! (aws-smithy-mocks) — the first AWS mock test exercising the inference edge.

use aws_sdk_bedrockruntime::Client;
use aws_sdk_bedrockruntime::operation::invoke_model::InvokeModelOutput;
use aws_smithy_mocks::{RuleMode, mock, mock_client};
use aws_smithy_types::Blob;
use vu_infer::{InferenceConfig, KeyMomentModel, PegasusClient};

const RESPONSE: &str = r#"{
    "segments": [
        { "id": "key_moment", "start_sec": 0, "end_sec": 12, "title": "Kickoff", "type": "scene_cut", "confidence": 0.4, "description": "The match begins." },
        { "id": "key_moment", "start_sec": 12, "end_sec": 30, "title": "Goal", "type": "highlight", "confidence": 0.9, "description": "A goal is scored from distance." }
    ],
    "definition_errors": [],
    "stopReason": "stop",
    "finish_reason": "stop"
}"#;

#[tokio::test]
async fn analyze_key_moments_parses_mocked_response() {
    let rule = mock!(Client::invoke_model)
        .match_requests(|r| r.model_id() == Some("us.twelvelabs.pegasus-1-5-v1:0"))
        .then_output(|| {
            InvokeModelOutput::builder()
                .body(Blob::new(RESPONSE.as_bytes().to_vec()))
                .content_type("application/json")
                .build()
                .unwrap()
        });
    let client = mock_client!(aws_sdk_bedrockruntime, RuleMode::Sequential, &[&rule]);

    let pegasus = PegasusClient::new(
        client,
        InferenceConfig {
            model_id: "us.twelvelabs.pegasus-1-5-v1:0".into(),
            max_output_tokens: 4096,
        },
    );

    let moments = pegasus
        .analyze_key_moments("Zm9vYmFy")
        .await
        .expect("inference should succeed");

    assert_eq!(moments.len(), 2);
    assert_eq!(moments[0].title, "Kickoff");
    assert_eq!(moments[0].start_sec, 0.0);
    assert_eq!(moments[1].title, "Goal");
    assert_eq!(moments[1].moment_type.as_deref(), Some("highlight"));
    assert_eq!(moments[1].confidence, Some(0.9));
}
