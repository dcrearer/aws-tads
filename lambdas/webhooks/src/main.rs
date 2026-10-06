//! Webhooks Lambda binary — bootstraps observability, builds state, and serves the router.

use tads_http::AppState;
use tads_lambda_webhooks::router;

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    tads_http::bootstrap();
    let state = AppState::from_env()
        .await
        .map_err(|e| lambda_http::Error::from(e.to_string()))?;
    tads_http::serve(router(state)).await
}
