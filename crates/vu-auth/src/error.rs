use thiserror::Error;

/// Errors from OAuth2 token acquisition and secret loading.
#[derive(Debug, Error)]
pub enum VuAuthError {
    #[error("secrets manager get_secret_value failed: {0}")]
    SecretsManager(String),
    #[error("client-credentials secret is malformed: {0}")]
    SecretParse(String),
    #[error("token endpoint request failed: {0}")]
    TokenRequest(String),
    #[error("token endpoint response could not be parsed: {0}")]
    TokenParse(String),
}
