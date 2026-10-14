use thiserror::Error;

/// Errors from the VU domain layer.
#[derive(Debug, Error)]
pub enum VuDomainError {
    /// A key-moment payload could not be serialized to JSON (should not happen for the typed
    /// payload, but surfaced rather than panicking at the edge).
    #[error("key-moment payload serialization failed: {0}")]
    Serialization(String),
}
