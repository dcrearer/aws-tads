use thiserror::Error;

/// Errors from the TADS write client.
#[derive(Debug, Error)]
pub enum TadsError {
    #[error("TADS request failed: {0}")]
    Http(String),
    #[error("TADS returned {status} for {url}")]
    Status { status: u16, url: String },
    #[error("TADS response could not be decoded: {0}")]
    Decode(String),
    #[error("acquiring TADS access token failed: {0}")]
    Auth(String),
    #[error("a delete request did not complete after {attempts} polls")]
    DeleteTimeout { attempts: u32 },
    #[error("delete request failed: {0}")]
    DeleteFailed(String),
}
