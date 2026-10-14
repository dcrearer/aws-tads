//! The Bedrock InvokeModel edge for Pegasus, behind a trait so the worker can be faked.

use std::future::Future;

use aws_smithy_types::Blob;

use crate::error::InferError;
use crate::protocol::{InferenceConfig, RawMoment, build_request, parse_response};

/// Key-moment extraction surface used by the analysis worker.
pub trait KeyMomentModel: Send + Sync {
    /// Analyse one base64-encoded video clip and return the raw moments found in it.
    fn analyze_key_moments(
        &self,
        media_base64: &str,
    ) -> impl Future<Output = Result<Vec<RawMoment>, InferError>> + Send;
}

/// Bedrock-backed Pegasus client.
#[derive(Clone)]
pub struct PegasusClient {
    client: aws_sdk_bedrockruntime::Client,
    cfg: InferenceConfig,
}

impl PegasusClient {
    pub fn new(client: aws_sdk_bedrockruntime::Client, cfg: InferenceConfig) -> Self {
        Self { client, cfg }
    }

    pub fn config(&self) -> &InferenceConfig {
        &self.cfg
    }
}

impl KeyMomentModel for PegasusClient {
    async fn analyze_key_moments(&self, media_base64: &str) -> Result<Vec<RawMoment>, InferError> {
        let body = build_request(media_base64, &self.cfg);
        let bytes = serde_json::to_vec(&body).map_err(|e| InferError::Request(e.to_string()))?;

        let output = self
            .client
            .invoke_model()
            .model_id(&self.cfg.model_id)
            .content_type("application/json")
            .accept("application/json")
            .body(Blob::new(bytes))
            .send()
            .await
            .map_err(|e| InferError::Bedrock(e.to_string()))?;

        let body = output.body().as_ref();
        // The raw response is small; log it so an empty/unexpected result can be diagnosed.
        tracing::debug!(bytes = body.len(), "pegasus response received");
        tracing::trace!(response = %String::from_utf8_lossy(body), "pegasus raw response");

        let moments = parse_response(body)?;
        tracing::debug!(moments = moments.len(), "pegasus parsed key moments");
        Ok(moments)
    }
}
