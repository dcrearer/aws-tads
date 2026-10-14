use thiserror::Error;

/// Errors from the TAMS client.
#[derive(Debug, Error)]
pub enum TamsError {
    #[error("TAMS request failed: {0}")]
    Http(String),
    #[error("TAMS returned {status} for {url}")]
    Status { status: u16, url: String },
    #[error("TAMS response could not be decoded: {0}")]
    Decode(String),
    #[error("acquiring TAMS access token failed: {0}")]
    Auth(String),
}
