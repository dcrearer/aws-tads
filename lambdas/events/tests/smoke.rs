//! End-to-end smoke test for the events Lambda router.
//!
//! Drives the real axum router (via `tower::ServiceExt::oneshot`) against `aws-smithy-mocks`-
//! backed AWS clients — the mandated AWS mocking seam. Verifies that a batched POST validates
//! each payload against the Track's Payload Schema: a conforming Event is created (201) while a
//! schema-violating Event is rejected per-item (400), with the overall request returning 200.

use aws_sdk_dynamodb::operation::get_item::GetItemOutput;
use aws_sdk_dynamodb::operation::put_item::PutItemOutput;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_sqs::operation::send_message::SendMessageOutput;
use aws_smithy_mocks::{RuleMode, mock, mock_client};
use axum::body::Body;
use http::Request;
use tads_http::AppState;
use tads_http::config::{AppConfig, ServiceInfo, Tables};
use tower::ServiceExt;

const TRACK_ID: &str = "2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f";
const SCHEMA_ID: &str = "3e2f3c4d-5b6c-4d7e-9faf-1b2c3d4e5f60";

fn test_config() -> AppConfig {
    AppConfig {
        tables: Tables {
            schemas: "tads-schemas".into(),
            tracks: "tads-tracks".into(),
            events: "tads-events".into(),
            webhooks: "tads-webhooks".into(),
            delete_requests: "tads-delete-requests".into(),
        },
        overflow_bucket: "tads-overflow".into(),
        delete_queue_url: "https://sqs.local/queue".into(),
        overflow_threshold_bytes: 360 * 1024,
        service: ServiceInfo {
            name: Some("test".into()),
            service_type: "urn:x-tads:service:test".into(),
            api_version: "0.1".into(),
            service_version: None,
        },
    }
}

fn mock_state() -> AppState {
    let track_doc = serde_json::json!({
        "id": TRACK_ID,
        "timeline_reference": { "entity_id": "tams://flow/abc" },
        "schema_id": SCHEMA_ID,
        "allow_overlaps": true,
        "editable_events": true
    })
    .to_string();

    let schema_doc = serde_json::json!({
        "id": SCHEMA_ID,
        "schema": {
            "type": "object",
            "required": ["text"],
            "properties": { "text": { "type": "string" } }
        }
    })
    .to_string();

    let track_rule = mock!(aws_sdk_dynamodb::Client::get_item)
        .match_requests(|req| req.table_name() == Some("tads-tracks"))
        .then_output(move || {
            GetItemOutput::builder()
                .item("doc", AttributeValue::S(track_doc.clone()))
                .build()
        });
    let schema_rule = mock!(aws_sdk_dynamodb::Client::get_item)
        .match_requests(|req| req.table_name() == Some("tads-schemas"))
        .then_output(move || {
            GetItemOutput::builder()
                .item("doc", AttributeValue::S(schema_doc.clone()))
                .build()
        });
    let put_rule =
        mock!(aws_sdk_dynamodb::Client::put_item).then_output(|| PutItemOutput::builder().build());

    let dynamo = mock_client!(
        aws_sdk_dynamodb,
        RuleMode::MatchAny,
        [&track_rule, &schema_rule, &put_rule]
    );

    let s3_rule =
        mock!(aws_sdk_s3::Client::put_object).then_output(|| PutObjectOutput::builder().build());
    let s3 = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&s3_rule]);

    let sqs_rule = mock!(aws_sdk_sqs::Client::send_message)
        .then_output(|| SendMessageOutput::builder().build());
    let sqs = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&sqs_rule]);

    let config = test_config();
    let store = config.store_config();
    AppState::from_parts(config, store, dynamo, s3, sqs)
}

#[tokio::test]
async fn post_events_validates_each_payload() {
    let state = mock_state();
    let app = tads_lambda_events::router(state);

    let body = serde_json::json!([
        { "timerange": "[0:0_10:0)", "payload": { "text": "hello" } },
        { "timerange": "[10:0_20:0)", "payload": { "text": 123 } }
    ])
    .to_string();

    let request = Request::builder()
        .method("POST")
        .uri(format!("/tracks/{TRACK_ID}/events"))
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200, "batch POST returns 200");

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(parsed["all_successful"], false);
    let responses = parsed["responses"].as_array().unwrap();
    assert_eq!(responses.len(), 2);
    assert_eq!(
        responses[0]["http_status_code"], 201,
        "valid payload created"
    );
    assert_eq!(
        responses[1]["http_status_code"], 400,
        "schema-violating payload rejected"
    );
}

#[tokio::test]
async fn get_events_returns_paging_headers() {
    // A track with no events still returns 200 with the X-Paging-* headers.
    let empty_track = serde_json::json!({
        "id": TRACK_ID,
        "timeline_reference": { "entity_id": "tams://flow/abc" },
        "schema_id": SCHEMA_ID
    })
    .to_string();

    let track_rule = mock!(aws_sdk_dynamodb::Client::get_item)
        .match_requests(|req| req.table_name() == Some("tads-tracks"))
        .then_output(move || {
            GetItemOutput::builder()
                .item("doc", AttributeValue::S(empty_track.clone()))
                .build()
        });
    let query_rule = mock!(aws_sdk_dynamodb::Client::query)
        .then_output(|| aws_sdk_dynamodb::operation::query::QueryOutput::builder().build());

    let dynamo = mock_client!(
        aws_sdk_dynamodb,
        RuleMode::MatchAny,
        [&track_rule, &query_rule]
    );
    let s3_rule =
        mock!(aws_sdk_s3::Client::put_object).then_output(|| PutObjectOutput::builder().build());
    let s3 = mock_client!(aws_sdk_s3, RuleMode::MatchAny, [&s3_rule]);
    let sqs_rule = mock!(aws_sdk_sqs::Client::send_message)
        .then_output(|| SendMessageOutput::builder().build());
    let sqs = mock_client!(aws_sdk_sqs, RuleMode::MatchAny, [&sqs_rule]);

    let config = test_config();
    let store = config.store_config();
    let state = AppState::from_parts(config, store, dynamo, s3, sqs);

    let request = Request::builder()
        .method("GET")
        .uri(format!("/tracks/{TRACK_ID}/events?timerange=[0:0_100:0)"))
        .body(Body::empty())
        .unwrap();

    let response = tads_lambda_events::router(state)
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.headers().contains_key("x-paging-limit"));
    assert!(response.headers().contains_key("x-paging-timerange"));
}
