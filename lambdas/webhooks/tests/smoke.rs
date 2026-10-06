//! End-to-end smoke test for the webhooks Lambda router.
//!
//! Verifies that creating a webhook returns 201 and that the write-only `api_key_value` is
//! accepted but never echoed back in the `WebhookGet` response.

use aws_sdk_dynamodb::operation::put_item::PutItemOutput;
use aws_sdk_sqs::operation::send_message::SendMessageOutput;
use aws_smithy_mocks::{RuleMode, mock, mock_client};
use axum::body::Body;
use http::Request;
use tads_http::AppState;
use tads_http::config::{AppConfig, ServiceInfo, Tables};
use tower::ServiceExt;

fn test_state() -> AppState {
    let put_rule =
        mock!(aws_sdk_dynamodb::Client::put_item).then_output(|| PutItemOutput::builder().build());
    let dynamo = mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&put_rule]);

    let sqs_rule = mock!(aws_sdk_sqs::Client::send_message)
        .then_output(|| SendMessageOutput::builder().build());
    let sqs = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&sqs_rule]);

    let config = AppConfig {
        tables: Tables {
            schemas: "tads-schemas".into(),
            tracks: "tads-tracks".into(),
            events: "tads-events".into(),
            webhooks: "tads-webhooks".into(),
            delete_requests: "tads-delete-requests".into(),
        },
        delete_queue_url: "https://sqs.local/queue".into(),
        max_payload_bytes: 360 * 1024,
        service: ServiceInfo {
            name: Some("test".into()),
            service_type: "urn:x-tads:service:test".into(),
            api_version: "0.1".into(),
            service_version: None,
        },
    };
    let store = config.store_config();
    AppState::from_parts(config, store, dynamo, sqs)
}

#[tokio::test]
async fn create_webhook_does_not_echo_api_key_value() {
    let app = tads_lambda_webhooks::router(test_state());

    let body = serde_json::json!({
        "url": "https://subscriber.example/hook",
        "events": ["tracks/created", "tracks/events_added"],
        "api_key_name": "x-api-key",
        "api_key_value": "super-secret"
    })
    .to_string();

    let request = Request::builder()
        .method("POST")
        .uri("/service/webhooks")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), 201, "webhook created");

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert!(
        parsed.get("id").is_some(),
        "response carries the generated id"
    );
    assert_eq!(parsed["status"], "created");
    assert_eq!(parsed["url"], "https://subscriber.example/hook");
    assert!(
        parsed.get("api_key_value").is_none(),
        "the write-only api_key_value must never be returned"
    );
}
