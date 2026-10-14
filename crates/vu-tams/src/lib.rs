//! TAMS integration: the `flows/segments_added` webhook model, constant-time signature
//! verification, the `tams://` ⇄ `https://` mapping, and the segment/media HTTP client.
//!
//! The models, signature check, and URI mapping are pure and unit-tested; the [`TamsClient`] is the
//! reqwest I/O edge and sits behind the [`TamsApi`] trait so the worker can be driven with a fake.

pub mod client;
pub mod error;
pub mod models;
pub mod signature;
pub mod uri;

pub use client::{TamsApi, TamsClient};
pub use error::TamsError;
pub use models::{FlowSegment, GetUrl, SegmentsAddedEvent};
pub use signature::verify_signature;
pub use uri::{flow_media_object_uri, https_to_tams, source_entity_id, tams_to_https};
