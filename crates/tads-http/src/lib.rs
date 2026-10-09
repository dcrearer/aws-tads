//! Shared axum glue for every TADS Lambda: central error type, auth extractor, pagination
//! headers, config/state wiring, an observability middleware, and the Lambda bootstrap.
//!
//! A request Lambda's `main` is tiny:
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<(), lambda_http::Error> {
//!     tads_http::bootstrap();
//!     let state = tads_http::AppState::from_env().await?;
//!     tads_http::serve(my_router(state)).await
//! }
//! ```

pub mod auth;
pub mod config;
pub mod error;
pub mod paging;
pub mod query;
pub mod state;

pub use auth::Principal;
pub use config::AppConfig;
pub use error::{ApiResult, AppError};
pub use paging::PagingHeaders;
pub use state::{AppState, connect_pool, poke_on_mutation};

// Re-export the store list primitives handlers need, so Lambda crates depend only on tads-http.
pub use tads_store::{Page, PageParams};

use axum::Router;
use axum::extract::Request;
use axum::middleware::{self, Next};
use axum::response::Response;
use tracing::Instrument;

/// Initialise observability. Call once at the top of every Lambda `main`.
pub fn bootstrap() {
    tads_telemetry::init();
}

/// Wrap a router so every request runs inside a span carrying method, route, and the X-Ray trace
/// id — the backbone of cross-Lambda operational tracing.
pub fn with_observability(router: Router) -> Router {
    router.layer(middleware::from_fn(span_middleware))
}

async fn span_middleware(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let xray = tads_telemetry::xray_trace_id();
    let span = tracing::info_span!(
        "request",
        http.method = %method,
        http.route = %path,
        xray_trace_id = xray.as_deref().unwrap_or("-"),
    );
    async move { next.run(req).await }.instrument(span).await
}

/// Serve an axum router on the Lambda runtime. Strips the API Gateway stage from the path so
/// routes match, then hands the router (a `tower::Service`) to the runtime.
pub async fn serve(app: Router) -> Result<(), lambda_http::Error> {
    // SAFETY: set before the runtime starts handling requests; single-threaded at this point.
    unsafe {
        std::env::set_var("AWS_LAMBDA_HTTP_IGNORE_STAGE_IN_PATH", "true");
    }
    lambda_http::run(with_observability(app)).await
}
