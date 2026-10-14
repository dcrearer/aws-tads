//! TwelveLabs Pegasus video-understanding inference on Amazon Bedrock.
//!
//! Pure request-builder and response-parser ([`protocol`]) with the live `InvokeModel` call behind
//! the [`KeyMomentModel`] trait ([`PegasusClient`]), so the structured-output contract is tested
//! without a live model and the worker can run against a fake.

pub mod client;
pub mod error;
pub mod protocol;

pub use client::{KeyMomentModel, PegasusClient};
pub use error::InferError;
pub use protocol::{
    InferenceConfig, KEY_MOMENT_SEGMENT_ID, RawMoment, build_request, parse_response,
};
