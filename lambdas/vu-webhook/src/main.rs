//! vu-webhook Lambda binary — bootstraps observability, builds state, and serves the router.

use tads_lambda_vu_webhook::{WebhookState, router};

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    tads_http::bootstrap();
    let state = WebhookState::from_env()
        .await
        .map_err(lambda_http::Error::from)?;
    tads_http::serve(router(state)).await
}
