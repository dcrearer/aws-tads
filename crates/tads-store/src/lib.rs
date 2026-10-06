//! DynamoDB-backed persistence for TADS (system-of-record).
//!
//! Event payloads are stored **inline** so they are returned directly in listings; S3 is a
//! transparent overflow for the occasional payload approaching DynamoDB's 400 KB item limit.
//! Repositories wrap the AWS SDK clients directly — tests mock at the SDK-client level with
//! `aws-smithy-mocks`, which is the mandated AWS mocking approach, so no trait objects are
//! needed. All tables have DynamoDB Streams enabled (infra) to feed the webhook dispatcher and
//! OpenSearch search-indexer.

pub mod config;
pub mod cursor;
pub mod error;
pub mod keys;

pub mod delete_requests;
pub mod events;
pub mod schemas;
pub mod tracks;
pub mod webhooks;

pub use config::StoreConfig;
pub use cursor::{Page, PageParams};
pub use error::StoreError;

pub use delete_requests::{DeleteRequestRepo, DeleteSortBy};
pub use events::{EventQuery, EventRepo};
pub use schemas::{SchemaQuery, SchemaRepo};
pub use tracks::{TrackQuery, TrackRepo};
pub use webhooks::{StoredWebhook, WebhookQuery, WebhookRepo};
