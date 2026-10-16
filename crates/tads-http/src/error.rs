//! Central API error type → HTTP responses.
//!
//! The Rust-API standard names actix's `ResponseError`; we use axum (the sanctioned
//! alternative), so the equivalent is `IntoResponse`. Every error renders the spec's
//! `error.json` body and emits a structured `tracing` event carrying the error type, so failures
//! are observable within the per-request span (which already holds the handler + X-Ray trace id).

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use serde_json::json;
use tads_domain::DomainError;
use tads_store::StoreError;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    Validation(String),
    #[error("{0}")]
    PayloadTooLarge(String),
    #[error("{0}")]
    Internal(String),
}

impl AppError {
    fn status(&self) -> StatusCode {
        match self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Forbidden(_) => StatusCode::FORBIDDEN,
            AppError::Validation(_) => StatusCode::BAD_REQUEST,
            AppError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Machine-readable `type` for the `error.json` body.
    fn kind(&self) -> &'static str {
        match self {
            AppError::NotFound(_) => "not_found",
            AppError::BadRequest(_) => "bad_request",
            AppError::Forbidden(_) => "forbidden",
            AppError::Validation(_) => "validation_error",
            AppError::PayloadTooLarge(_) => "payload_too_large",
            AppError::Internal(_) => "internal_error",
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        let kind = self.kind();
        let summary = self.to_string();

        // Observability: emit the failure within the active request span.
        if status.is_server_error() {
            tracing::error!(error.type = kind, error.summary = %summary, "request failed");
        } else {
            tracing::warn!(error.type = kind, error.summary = %summary, "request rejected");
        }

        let body = json!({
            "type": kind,
            "summary": summary,
            "time": Utc::now().to_rfc3339(),
        });
        (status, Json(body)).into_response()
    }
}

impl From<DomainError> for AppError {
    fn from(e: DomainError) -> Self {
        match e {
            DomainError::InvalidTimeRange { .. } | DomainError::InvalidUuid(_) => {
                AppError::BadRequest(e.to_string())
            }
            DomainError::InvalidSchema(_)
            | DomainError::InvalidTimelineReference(_)
            | DomainError::InvalidWebhookUrl(_) => AppError::BadRequest(e.to_string()),
            DomainError::PayloadValidation(_) => AppError::Validation(e.to_string()),
        }
    }
}

impl From<StoreError> for AppError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::Cursor(_) => AppError::BadRequest(e.to_string()),
            // TADS v0.1 uses `400` for client conflicts (e.g. duplicate schema `external_id`);
            // the spec defines no `409` on any operation.
            StoreError::Conflict(_) => AppError::BadRequest(e.to_string()),
            StoreError::PayloadTooLarge { .. } => AppError::PayloadTooLarge(e.to_string()),
            other => AppError::Internal(other.to_string()),
        }
    }
}

/// Convenience alias for handler results.
pub type ApiResult<T> = Result<T, AppError>;
