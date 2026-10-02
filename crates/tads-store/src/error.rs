use thiserror::Error;

/// Errors from the persistence layer. SDK errors are flattened to strings at the call site
/// (their types are generic over operation) so this stays a simple, matchable enum.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("dynamodb error: {0}")]
    Dynamo(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("invalid pagination cursor: {0}")]
    Cursor(String),

    #[error("malformed stored item: {0}")]
    Malformed(String),

    #[error("event payload too large: {size} bytes exceeds the {limit}-byte limit")]
    PayloadTooLarge { size: usize, limit: usize },
}

impl StoreError {
    pub fn dynamo(e: impl std::fmt::Display) -> Self {
        StoreError::Dynamo(e.to_string())
    }
}
