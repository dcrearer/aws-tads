//! TADS write client for the video-understanding pipeline.
//!
//! Ensures the key-moment Payload Schema and Track exist, posts Events, and runs the segment-scoped
//! delete→poll→recreate re-analysis path. The resource builders and the per-event response
//! interpretation are pure and unit-tested; [`TadsClient`] is the reqwest I/O edge behind the
//! [`TadsApi`] trait so the worker can be driven with a fake.

pub mod build;
pub mod client;
pub mod error;
pub mod events;

pub use build::{key_moment_schema, key_moment_track};
pub use client::{DeleteHandle, TadsApi, TadsClient, absolutize};
pub use error::TadsError;
pub use events::{EventOutcome, interpret};
