use thiserror::Error;

/// Errors from the persistence layer. Backend errors (sqlx/SQS) are flattened to strings at the
/// call site so this stays a simple, matchable enum that `tads-http` maps to HTTP statuses.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("invalid pagination cursor: {0}")]
    Cursor(String),

    #[error("malformed stored row: {0}")]
    Malformed(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("event payload too large: {size} bytes exceeds the {limit}-byte limit")]
    PayloadTooLarge { size: usize, limit: usize },
}

impl StoreError {
    /// Flatten any backend error (sqlx, SQS SDK, …) into [`StoreError::Database`].
    pub fn backend(e: impl std::fmt::Display) -> Self {
        StoreError::Database(e.to_string())
    }

    pub fn serialization(e: impl std::fmt::Display) -> Self {
        StoreError::Serialization(e.to_string())
    }
}

impl From<sqlx::Error> for StoreError {
    fn from(e: sqlx::Error) -> Self {
        // A unique-constraint violation (e.g. a duplicate schema `external_id`) is a client
        // conflict, not an internal error.
        if let sqlx::Error::Database(ref db) = e
            && db.is_unique_violation()
        {
            return StoreError::Conflict(db.message().to_string());
        }
        StoreError::Database(e.to_string())
    }
}
