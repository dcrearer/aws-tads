use thiserror::Error;

/// Errors produced by the pure domain layer (parsing, validation, invariants).
///
/// Library crates use `thiserror` (never a manual `impl Error`). The HTTP layer maps these
/// onto `tads_http::AppError` / status codes.
#[derive(Debug, Error)]
pub enum DomainError {
    #[error("invalid timerange `{input}`: {reason}")]
    InvalidTimeRange { input: String, reason: String },

    #[error("invalid uuid `{0}`")]
    InvalidUuid(String),

    #[error("payload failed schema validation: {0}")]
    PayloadValidation(String),

    #[error("invalid payload schema: {0}")]
    InvalidSchema(String),

    #[error("invalid timeline_reference: {0}")]
    InvalidTimelineReference(String),
}
