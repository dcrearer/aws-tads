//! Search-indexer binary — wires config/OpenSearch to the [`Indexer`] and consumes the stream.

use std::sync::Arc;

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;
use tads_http::AppConfig;
use tads_lambda_search_indexer::{Indexer, Indices, OpenSearchSink};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();

    let config = AppConfig::load().map_err(|e| Error::from(e.to_string()))?;
    let endpoint = std::env::var("TADS_OPENSEARCH_ENDPOINT")
        .map_err(|_| Error::from("TADS_OPENSEARCH_ENDPOINT is not set"))?;

    let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let sink = OpenSearchSink::new(&endpoint, &sdk).map_err(|e| Error::from(e.to_string()))?;
    let indices = Indices {
        schemas: config.tables.schemas.clone(),
        tracks: config.tables.tracks.clone(),
        events: config.tables.events.clone(),
    };
    let indexer = Arc::new(Indexer::new(sink, indices));

    lambda_runtime::run(service_fn(move |event: LambdaEvent<Value>| {
        let indexer = indexer.clone();
        async move {
            let records = event
                .payload
                .get("Records")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            tracing::info!(records = records.len(), "search-indexer batch received");
            if let Err(e) = indexer.index_batch(&records).await {
                tracing::error!(error = %e, "indexing failed");
            }
            Ok::<(), Error>(())
        }
    }))
    .await
}
