//! Schemas Lambda — Payload Schema registry (`/schemas/**`).
//!
//! Phase 1 implements list + CRUD + the immutable `schema` sub-resource. The granular metadata
//! sub-resources (`/tags/{name}`, `/description`, `/label`, `/external_id`) are 501 stubs filled
//! in Phase 2.

use std::collections::HashMap;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use chrono::Utc;
use tads_domain::PayloadSchema;
use tads_http::error::ApiResult;
use tads_http::{AppError, AppState, PagingHeaders, Principal};
use tads_store::SchemaQuery;

fn router(state: AppState) -> Router {
    Router::new()
        .route("/schemas", get(list_schemas))
        .route(
            "/schemas/{schema_id}",
            get(get_schema).put(put_schema).delete(delete_schema),
        )
        .route("/schemas/{schema_id}/schema", get(get_schema_body))
        // Granular metadata sub-resources — Phase 2.
        .route(
            "/schemas/{schema_id}/tags",
            get(todo_501).put(todo_501).delete(todo_501),
        )
        .route(
            "/schemas/{schema_id}/tags/{name}",
            get(todo_501).put(todo_501).delete(todo_501),
        )
        .route(
            "/schemas/{schema_id}/description",
            get(todo_501).put(todo_501).delete(todo_501),
        )
        .route(
            "/schemas/{schema_id}/label",
            get(todo_501).put(todo_501).delete(todo_501),
        )
        .route(
            "/schemas/{schema_id}/external_id",
            get(todo_501).put(todo_501).delete(todo_501),
        )
        .with_state(state)
}

#[tracing::instrument(skip_all, fields(handler = "list_schemas"))]
async fn list_schemas(
    State(state): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let (tag_matches, tag_exists) = tads_http::query::tag_filters(&q);
    let query = SchemaQuery {
        label: q.get("label").cloned(),
        external_id: q.get("external_id").cloned(),
        tag_matches,
        tag_exists,
    };
    let page = tads_http::query::page_params(&q);
    let result = state.schemas.list(&query, &page).await?;

    let headers = PagingHeaders::new(&result, "/schemas").into_header_map();
    Ok((headers, Json(result.items)).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_schema", schema_id = %id))]
async fn get_schema(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Response> {
    match state.schemas.get(&id).await? {
        Some(schema) => Ok(Json(schema).into_response()),
        None => Err(AppError::NotFound(format!("schema {id} not found"))),
    }
}

#[tracing::instrument(skip_all, fields(handler = "get_schema_body", schema_id = %id))]
async fn get_schema_body(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    match state.schemas.get(&id).await? {
        Some(schema) => Ok(Json(schema.schema).into_response()),
        None => Err(AppError::NotFound(format!("schema {id} not found"))),
    }
}

#[tracing::instrument(skip_all, fields(handler = "put_schema", schema_id = %id))]
async fn put_schema(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
    Json(mut body): Json<PayloadSchema>,
) -> ApiResult<Response> {
    let tid = id
        .parse()
        .map_err(|_| AppError::BadRequest(format!("invalid schema id `{id}`")))?;
    body.id = tid;

    // The `schema` body must be a usable JSON Schema.
    tads_domain::ensure_valid_schema(&body.schema)?;

    let now = Utc::now();
    let existed = match state.schemas.get(&id).await? {
        Some(existing) => {
            // The `schema` value is immutable once set.
            if existing.schema != body.schema {
                return Err(AppError::Forbidden(
                    "cannot modify an existing Payload Schema's `schema` value".into(),
                ));
            }
            body.created = existing.created;
            body.created_by = existing.created_by;
            true
        }
        None => {
            body.created = Some(now);
            body.created_by = principal.actor().map(str::to_string);
            false
        }
    };
    body.updated = Some(now);
    body.updated_by = principal.actor().map(str::to_string);

    state.schemas.put(&body).await?;

    if existed {
        Ok(StatusCode::NO_CONTENT.into_response())
    } else {
        Ok((StatusCode::CREATED, Json(body)).into_response())
    }
}

#[tracing::instrument(skip_all, fields(handler = "delete_schema", schema_id = %id))]
async fn delete_schema(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    if state.schemas.get(&id).await?.is_none() {
        return Err(AppError::NotFound(format!("schema {id} not found")));
    }
    if state.tracks.references_schema(&id).await? {
        return Err(AppError::BadRequest(
            "schema is still referenced by one or more tracks".into(),
        ));
    }
    state.schemas.delete(&id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn todo_501() -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    tads_http::bootstrap();
    let state = AppState::from_env()
        .await
        .map_err(|e| lambda_http::Error::from(e.to_string()))?;
    tads_http::serve(router(state)).await
}
