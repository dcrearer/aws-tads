//! Track Delete Requests Lambda — poll async deletion jobs (`/track-delete-requests/**`).

use std::collections::HashMap;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use tads_http::error::ApiResult;
use tads_http::{AppError, AppState, PagingHeaders};
use tads_store::DeleteSortBy;

fn router(state: AppState) -> Router {
    Router::new()
        .route("/track-delete-requests", get(list_requests))
        .route("/track-delete-requests/{request_id}", get(get_request))
        .with_state(state)
}

#[tracing::instrument(skip_all, fields(handler = "list_delete_requests"))]
async fn list_requests(
    State(state): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let sort_by = match q.get("sort_by").map(String::as_str) {
        Some("expiry") => DeleteSortBy::Expiry,
        Some("created") | None => DeleteSortBy::Created,
        Some(other) => return Err(AppError::BadRequest(format!("invalid sort_by `{other}`"))),
    };
    let reverse_order = q
        .get("reverse_order")
        .map(|v| tads_http::query::parse_bool(v))
        .unwrap_or(false);
    let page = tads_http::query::page_params(&q);

    let result = state
        .delete_requests
        .list(sort_by, reverse_order, &page)
        .await?;
    let headers = PagingHeaders::new(&result, "/track-delete-requests")
        .with_reverse_order(reverse_order)
        .into_header_map();
    Ok((headers, Json(result.items)).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_delete_request", request_id = %id))]
async fn get_request(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Response> {
    match state.delete_requests.get(&id).await? {
        Some(req) => Ok(Json(req).into_response()),
        None => Err(AppError::NotFound(format!("delete request {id} not found"))),
    }
}

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    tads_http::bootstrap();
    let state = AppState::from_env()
        .await
        .map_err(|e| lambda_http::Error::from(e.to_string()))?;
    tads_http::serve(router(state)).await
}
