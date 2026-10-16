//! Webhooks Lambda library — subscription CRUD (`/service/webhooks/**`).
//!
//! Delivery is handled separately by the webhook relay/delivery pipeline (poke-driven outbox).
//! This Lambda owns registration: create/list/get/replace/delete. `api_key_value` is write-only —
//! accepted on
//! POST/PUT, stored out-of-band, and never returned in a `WebhookGet`. The router is exposed as a
//! library so it can be driven directly in integration tests.

use std::collections::HashMap;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use tads_domain::{TadsId, WebhookGet, WebhookPost, WebhookStatus};
use tads_http::error::ApiResult;
use tads_http::{AppError, AppState, PagingHeaders};
use tads_store::WebhookQuery;

/// Build the webhooks router bound to the given application state.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/service/webhooks", get(list_webhooks).post(create_webhook))
        .route(
            "/service/webhooks/{webhook_id}",
            get(get_webhook).put(put_webhook).delete(delete_webhook),
        )
        .with_state(state)
}

#[tracing::instrument(skip_all, fields(handler = "list_webhooks"))]
async fn list_webhooks(
    State(state): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let (tag_matches, tag_exists) = tads_http::query::tag_filters(&q);
    let query = WebhookQuery {
        tag_matches,
        tag_exists,
    };
    let page = tads_http::query::page_params(&q);
    let result = state.webhooks.list(&query, &page).await?;

    let headers = PagingHeaders::new(&result, "/service/webhooks").into_header_map();
    Ok((headers, Json(result.items)).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "create_webhook"))]
async fn create_webhook(
    State(state): State<AppState>,
    Json(post): Json<WebhookPost>,
) -> ApiResult<Response> {
    // SSRF guard: only https, public-routable subscriber URLs. The delivery worker re-checks the
    // resolved address before each POST (defends against DNS-rebinding).
    tads_domain::validate_webhook_url(&post.webhook.url)?;
    let id = TadsId::new_v4();
    let api_key_value = post.api_key_value.clone();
    let webhook = to_webhook_get(id, post);
    state
        .webhooks
        .put(&webhook, api_key_value.as_deref())
        .await?;
    Ok((StatusCode::CREATED, Json(webhook)).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_webhook", webhook_id = %id))]
async fn get_webhook(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Response> {
    match state.webhooks.get(&id).await? {
        Some(webhook) => Ok(Json(webhook).into_response()),
        None => Err(AppError::NotFound(format!("webhook {id} not found"))),
    }
}

#[tracing::instrument(skip_all, fields(handler = "put_webhook", webhook_id = %id))]
async fn put_webhook(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(post): Json<WebhookPost>,
) -> ApiResult<Response> {
    let tid: TadsId = id
        .parse()
        .map_err(|_| AppError::BadRequest(format!("invalid webhook id `{id}`")))?;
    tads_domain::validate_webhook_url(&post.webhook.url)?;
    let existed = state.webhooks.exists(&id).await?;
    let api_key_value = post.api_key_value.clone();
    let webhook = to_webhook_get(tid, post);
    state
        .webhooks
        .put(&webhook, api_key_value.as_deref())
        .await?;

    if existed {
        Ok(Json(webhook).into_response())
    } else {
        Ok((StatusCode::CREATED, Json(webhook)).into_response())
    }
}

#[tracing::instrument(skip_all, fields(handler = "delete_webhook", webhook_id = %id))]
async fn delete_webhook(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    if !state.webhooks.exists(&id).await? {
        return Err(AppError::NotFound(format!("webhook {id} not found")));
    }
    state.webhooks.delete(&id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Build the stored/GET form from a POST/PUT body. A `disabled` request stays disabled; anything
/// else starts in `created`. (A webhook whose deliveries exhaust retries is later driven to
/// `error` by the delivery DLQ handler; `started` is accepted as an active status but not
/// currently assigned by the pipeline.)
fn to_webhook_get(id: TadsId, post: WebhookPost) -> WebhookGet {
    let status = match post.status {
        Some(WebhookStatus::Disabled) => WebhookStatus::Disabled,
        _ => WebhookStatus::Created,
    };
    WebhookGet {
        id,
        webhook: post.webhook,
        status,
        error: None,
    }
}
