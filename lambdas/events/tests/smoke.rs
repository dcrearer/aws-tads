//! End-to-end smoke test for the events Lambda router against a real Postgres.
//!
//! Gated on `TADS_TEST_DB` (a Postgres connection string); skips when unset so `cargo test` is
//! green anywhere. Drives the real axum router via `tower::ServiceExt::oneshot` against the
//! Postgres-backed store, verifying per-payload schema validation (201 vs per-item 400), the 413
//! oversize guard, and the `X-Paging-*` headers.

use aws_config::BehaviorVersion;
use axum::body::Body;
use http::Request;
use tads_domain::{PayloadSchema, Track};
use tads_http::AppState;
use tads_http::config::{AppConfig, DatabaseSettings, ServiceInfo};
use tads_store::{SchemaRepo, TrackRepo};
use tower::ServiceExt;

const TRACK_ID: &str = "2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f";
const SCHEMA_ID: &str = "3e2f3c4d-5b6c-4d7e-9faf-1b2c3d4e5f60";

fn test_config() -> AppConfig {
    AppConfig {
        database: DatabaseSettings {
            proxy_endpoint: String::new(),
            port: 5432,
            db_name: "tads".into(),
            username: "tads".into(),
            secret_arn: String::new(),
            max_connections: 5,
        },
        delete_queue_url: "https://sqs.local/queue".into(),
        max_payload_bytes: 360 * 1024,
        service: ServiceInfo {
            name: Some("test".into()),
            service_type: "urn:x-tads:service:test".into(),
            api_version: "0.1".into(),
            service_version: None,
        },
    }
}

/// Build state against the test DB, migrate, reset tables, and seed a schema + track. Returns
/// `None` (skip) when `TADS_TEST_DB` is unset.
async fn harness() -> Option<AppState> {
    let url = std::env::var("TADS_TEST_DB").ok()?;
    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("connect TADS_TEST_DB");
    tads_store::migrate(&pool).await.expect("run migrations");
    sqlx::query(
        "TRUNCATE payload_schema, track, track_event, webhook, track_delete_request, outbox",
    )
    .execute(&pool)
    .await
    .expect("reset tables");

    let schema: PayloadSchema = serde_json::from_value(serde_json::json!({
        "id": SCHEMA_ID,
        "schema": { "type": "object", "required": ["text"], "properties": { "text": { "type": "string" } } }
    }))
    .unwrap();
    SchemaRepo::new(pool.clone()).put(&schema).await.unwrap();

    let track: Track = serde_json::from_value(serde_json::json!({
        "id": TRACK_ID,
        "timeline_reference": { "entity_id": "tams://flow/abc" },
        "schema_id": SCHEMA_ID,
        "allow_overlaps": true,
        "editable_events": true
    }))
    .unwrap();
    TrackRepo::new(pool.clone()).put(&track).await.unwrap();

    let aws = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let sqs = aws_sdk_sqs::Client::new(&aws);
    Some(AppState::from_parts(test_config(), pool, sqs))
}

#[tokio::test]
async fn post_events_validates_each_payload() {
    let Some(state) = harness().await else {
        eprintln!("skipping: set TADS_TEST_DB to a Postgres URL");
        return;
    };
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
async fn post_oversized_payload_rejected_with_413() {
    let Some(state) = harness().await else {
        eprintln!("skipping: set TADS_TEST_DB to a Postgres URL");
        return;
    };
    let app = tads_lambda_events::router(state);
    let big_text = "x".repeat(400 * 1024);
    let body = serde_json::json!([{ "timerange": "[0:0_10:0)", "payload": { "text": big_text } }])
        .to_string();

    let request = Request::builder()
        .method("POST")
        .uri(format!("/tracks/{TRACK_ID}/events"))
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200, "batch POST still returns 200");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed["all_successful"], false);
    let responses = parsed["responses"].as_array().unwrap();
    assert_eq!(
        responses[0]["http_status_code"], 413,
        "oversized payload rejected"
    );
}

#[tokio::test]
async fn get_events_returns_paging_headers() {
    let Some(state) = harness().await else {
        eprintln!("skipping: set TADS_TEST_DB to a Postgres URL");
        return;
    };
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
