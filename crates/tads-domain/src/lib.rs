//! Pure, AWS-free domain layer for the TADS API.
//!
//! TADS is the timeline-linked **metadata** store for content whose essence lives in TAMS.
//! This crate holds the serde models that mirror the `bbc/tads` v0.1 schemas, the TAI
//! [`TimeRange`] grammar (the single source of truth for the store's sort-key encoding), and
//! JSON-Schema payload validation. It has no I/O and is fully unit-testable.

pub mod error;
pub mod ids;
pub mod models;
pub mod tags;
pub mod timerange;
pub mod validation;

pub use error::DomainError;
pub use ids::TadsId;
pub use tags::{TagValue, Tags};
pub use timerange::{TimeRange, Timestamp};
pub use validation::{
    PayloadValidator, compile_schema, ensure_valid_schema, is_blocked_ip, validate_payload,
    validate_timeline_reference, validate_webhook_url, validate_with,
};

pub use models::{
    DeleteStatus, EditRate, ErrorBody, EventPostResponse, EventPostResponses, PayloadSchema,
    Service, TimelineReference, Track, TrackDeleteRequest, TrackEvent, TrackEventPost, Webhook,
    WebhookEvent, WebhookGet, WebhookPost, WebhookStatus,
};
