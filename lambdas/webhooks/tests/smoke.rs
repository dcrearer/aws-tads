//! End-to-end smoke test for the webhooks Lambda router against a real Postgres.
//!
//! Gated on `TADS_TEST_DB`; skips when unset. Verifies that creating a webhook returns 201 and
//! that the write-only `api_key_value` is accepted but never echoed back in the response.

use aws_config::BehaviorVersion;
use axum::body::Body;
use http::Request;
use tads_http::AppState;
use tads_http::config::{AppConfig, DatabaseSettings, ServiceInfo};
use tower::ServiceExt;

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
        poke_queue_url: None,
        delivery_queue_url: None,
        max_payload_bytes: 360 * 1024,
        service: ServiceInfo {
            name: Some("test".into()),
            service_type: "urn:x-tads:service:test".into(),
            api_version: "0.1".into(),
            service_version: None,
        },
    }
}

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
    let aws = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let sqs = aws_sdk_sqs::Client::new(&aws);
    Some(AppState::from_parts(test_config(), pool, sqs))
}

#[tokio::test]
async fn create_webhook_does_not_echo_api_key_value() {
    let Some(state) = harness().await else {
        eprintln!("skipping: set TADS_TEST_DB to a Postgres URL");
        return;
    };
    let app = tads_lambda_webhooks::router(state);

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
