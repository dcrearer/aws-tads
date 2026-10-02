//! Webhooks Lambda — subscription CRUD (`/service/webhooks/**`).
//!
//! Phase 2: full CRUD plus delivery wiring (the webhook-dispatcher consumes DynamoDB Streams).
//! Phase 1 ships the routes as 501 stubs so the surface area is visible and routable.

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;

fn router() -> Router {
    Router::new()
        .route("/service/webhooks", get(todo_501).post(todo_501))
        .route(
            "/service/webhooks/{webhook_id}",
            get(todo_501).put(todo_501).delete(todo_501),
        )
}

async fn todo_501() -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    tads_http::bootstrap();
    tads_http::serve(router()).await
}
