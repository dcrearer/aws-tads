//! Drives the vu-webhook router directly (tower oneshot) with a mocked SQS client. Verifies the
//! signature gate (401), payload validation (400), and the happy path (202 + enqueue).

use aws_sdk_sqs::Client;
use aws_sdk_sqs::operation::send_message::SendMessageOutput;
use aws_smithy_mocks::{RuleMode, mock, mock_client};
use axum::body::Body;
use hmac::{Hmac, Mac};
use http::Request;
use sha2::Sha256;
use tads_lambda_vu_webhook::{WebhookState, router};
use tower::ServiceExt;

const KEY: &[u8] = b"test-signing-key";
const QUEUE: &str = "https://sqs.local/q/vu-analysis";

fn sign(body: &[u8]) -> String {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(KEY).unwrap();
    mac.update(body);
    let tag = mac.finalize().into_bytes();
    format!(
        "sha256={}",
        tag.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}

fn state() -> WebhookState {
    let rule = mock!(Client::send_message)
        .match_requests(|r| r.queue_url() == Some(QUEUE))
        .then_output(|| SendMessageOutput::builder().message_id("m-1").build());
    let sqs = mock_client!(aws_sdk_sqs, RuleMode::Sequential, &[&rule]);
    WebhookState::new(sqs, QUEUE, KEY.to_vec(), "x-tams-signature")
}

fn post(sig: Option<&str>, body: &str) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri("/webhooks/tams/segments-added");
    if let Some(s) = sig {
        b = b.header("x-tams-signature", s);
    }
    b.body(Body::from(body.to_string())).unwrap()
}

#[tokio::test]
async fn valid_signature_enqueues_and_returns_202() {
    let body = r#"{"flow_id":"f-1","source_id":"s-1","segments":[{"object_id":"o","timerange":"[10:0_12:0)","get_urls":[{"url":"https://m/o"}]}]}"#;
    let sig = sign(body.as_bytes());
    let resp = router(state())
        .oneshot(post(Some(&sig), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
}

#[tokio::test]
async fn missing_signature_is_401() {
    let body = r#"{"flow_id":"f-1","segments":[]}"#;
    let resp = router(state()).oneshot(post(None, body)).await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn bad_signature_is_401() {
    let body = r#"{"flow_id":"f-1","segments":[]}"#;
    let resp = router(state())
        .oneshot(post(Some("sha256=deadbeef"), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn valid_signature_but_malformed_payload_is_400() {
    // Correctly signed, but not a segments_added event (no flow_id).
    let body = r#"{"not":"a-segments-event"}"#;
    let sig = sign(body.as_bytes());
    let resp = router(state())
        .oneshot(post(Some(&sig), body))
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}
