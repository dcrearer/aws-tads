use thiserror::Error;

/// Errors from Pegasus inference.
#[derive(Debug, Error)]
pub enum InferError {
    #[error("bedrock invoke_model failed: {0}")]
    Bedrock(String),
    #[error("request body could not be serialized: {0}")]
    Request(String),
    #[error("pegasus response could not be parsed: {0}")]
    Response(String),
}
