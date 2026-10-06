//! Service Lambda — the API root and service-description endpoints.
//!
//! Routes: `GET|HEAD /` (index of top-level sub-paths) and `GET|HEAD /service`
//! (→ `service.json`). axum answers HEAD automatically for GET routes.

use axum::Router;
use axum::extract::State;
use axum::response::Json;
use axum::routing::get;
use tads_http::AppState;

fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/service", get(service))
        .with_state(state)
}

/// `GET /` — the list of available top-level sub-paths (alphabetical, matching the reference
/// deployments' bare-name form).
async fn root() -> Json<serde_json::Value> {
    Json(serde_json::json!([
        "schemas",
        "service",
        "track-delete-requests",
        "tracks",
    ]))
}

/// `GET /service` — the service description.
async fn service(State(state): State<AppState>) -> Json<tads_domain::Service> {
    Json((*state.service).clone())
}

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    tads_http::bootstrap();
    let state = AppState::from_env()
        .await
        .map_err(|e| lambda_http::Error::from(e.to_string()))?;
    tads_http::serve(router(state)).await
}
