//! Postgres-backed persistence for TADS (system-of-record).
//!
//! Each aggregate is a table whose authoritative column is `doc jsonb` (the serialized domain
//! object), with scalar/`int8range` columns projected for keys, filters, and the timerange GiST
//! overlap index. Repositories hold a `sqlx::PgPool` (and the delete-request repo an SQS client).
//! Change-data-capture uses a transactional [`outbox`] drained by the `outbox-worker` Lambda
//! (replacing DynamoDB Streams); the outbox row is written in the same transaction as the data
//! change, so capture is atomic. Rich list filtering (tags/entity/timerange) is pushed into SQL
//! and JSONB predicates, which also delivers the spec's "search by the data" pillar natively.

pub mod config;
pub mod cursor;
pub mod db;
pub mod error;
pub mod keys;
pub mod outbox;

pub mod delete_requests;
pub mod events;
pub mod schemas;
pub mod tracks;
pub mod webhooks;

pub use config::{DatabaseConfig, StoreConfig};
pub use cursor::{Page, PageParams};
pub use db::{connect, migrate};
pub use error::StoreError;
pub use outbox::{OutboxRepo, OutboxRow};

pub use delete_requests::{DeleteRequestRepo, DeleteSortBy};
pub use events::{EventQuery, EventRepo};
pub use schemas::{SchemaQuery, SchemaRepo};
pub use tracks::{TrackQuery, TrackRepo};
pub use webhooks::{StoredWebhook, WebhookQuery, WebhookRepo};
