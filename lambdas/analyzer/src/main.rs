//! Analyzer Lambda (SQS-triggered): the Phase B per-segment loop.
//!
//! Cold start builds the shared clients (reqwest, OAuth token providers for TADS/TAMS, Bedrock,
//! ffmpeg); each SQS message is a TAMS `flows/segments_added` event. Per-message failures are
//! reported via `batchItemFailures` so SQS retries only the failed messages and DLQs the rest.
//! Runs outside the VPC (Bedrock/TADS/TAMS are public HTTPS).

use std::sync::Arc;

use aws_config::BehaviorVersion;
use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::{Value, json};
use tads_lambda_analyzer::{Analyzer, InferenceConfig, parse_job};
use vu_auth::{ClientCredentials, TokenProvider};
use vu_infer::KeyMomentAnalyzer;
use vu_media::Ffmpeg;
use vu_tads::TadsClient;
use vu_tams::TamsClient;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();

    let cfg = InferenceConfig::load().map_err(|e| Error::from(e.to_string()))?;
    let aws = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let http = reqwest::Client::builder()
        .build()
        .map_err(|e| Error::from(e.to_string()))?;

    let tads_creds = ClientCredentials::from_secret(
        &aws,
        &cfg.tads_secret_arn,
        &cfg.tads_token_url,
        &cfg.tads_scope,
    )
    .await?;
    let tads = TadsClient::new(
        http.clone(),
        &cfg.tads_base_url,
        TokenProvider::new(http.clone(), tads_creds),
    );

    let tams_creds = ClientCredentials::from_secret(
        &aws,
        &cfg.tams_secret_arn,
        &cfg.tams_token_url,
        &cfg.tams_scope,
    )
    .await?;
    let tams = TamsClient::new(http.clone(), TokenProvider::new(http.clone(), tams_creds));

    let bedrock = aws_sdk_bedrockruntime::Client::new(&aws);
    let analyzer = Arc::new(Analyzer {
        tams,
        tads,
        infer: KeyMomentAnalyzer::new(bedrock, cfg.bedrock_model_id.clone()),
        ffmpeg: Ffmpeg::from_env(),
        model_id: cfg.bedrock_model_id,
        frames_per_second: cfg.frames_per_second,
    });

    lambda_runtime::run(service_fn(move |event: LambdaEvent<Value>| {
        let analyzer = analyzer.clone();
        async move { handle(&analyzer, event).await }
    }))
    .await
}

#[tracing::instrument(skip_all, fields(handler = "analyzer"))]
async fn handle(analyzer: &Analyzer, event: LambdaEvent<Value>) -> Result<Value, Error> {
    let records = event
        .payload
        .get("Records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut failures = Vec::new();
    for record in &records {
        let msg_id = record
            .get("messageId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if let Err(e) = process_record(analyzer, record).await {
            tracing::warn!(message_id = %msg_id, error = %e, "segment processing failed; SQS will retry");
            failures.push(json!({ "itemIdentifier": msg_id }));
        }
    }
    Ok(json!({ "batchItemFailures": failures }))
}

async fn process_record(analyzer: &Analyzer, record: &Value) -> Result<(), String> {
    let body = record
        .get("body")
        .and_then(Value::as_str)
        .ok_or("SQS record missing string body")?;
    let event = parse_job(body).map_err(|e| format!("invalid segments_added job: {e}"))?;
    let written = analyzer
        .process_event(&event)
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(flow_id = %event.flow_id, events_written = written, "segment(s) analysed");
    Ok(())
}
