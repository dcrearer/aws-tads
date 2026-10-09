//! Schemas Lambda — Payload Schema registry (`/schemas/**`).
//!
//! Implements list + CRUD, the immutable `schema` sub-resource, and the granular metadata
//! sub-resources (`/tags`, `/tags/{name}`, `/description`, `/label`, `/external_id`).

use std::collections::HashMap;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use chrono::Utc;
use tads_domain::{PayloadSchema, TagValue};
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
        .route("/schemas/{schema_id}/tags", get(get_tags))
        .route(
            "/schemas/{schema_id}/tags/{name}",
            get(get_tag).put(put_tag).delete(delete_tag),
        )
        .route(
            "/schemas/{schema_id}/description",
            get(get_description)
                .put(put_description)
                .delete(delete_description),
        )
        .route(
            "/schemas/{schema_id}/label",
            get(get_label).put(put_label).delete(delete_label),
        )
        .route(
            "/schemas/{schema_id}/external_id",
            get(get_external_id)
                .put(put_external_id)
                .delete(delete_external_id),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            tads_http::poke_on_mutation,
        ))
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

// ---- Granular metadata sub-resources ---------------------------------------------------------
//
// Each loads the schema (404 if absent), mutates one field, bumps the server-managed `updated`
// metadata, and re-persists. The immutable `schema` body is never touched here.

async fn load(state: &AppState, id: &str) -> ApiResult<PayloadSchema> {
    state
        .schemas
        .get(id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("schema {id} not found")))
}

async fn save(state: &AppState, principal: &Principal, mut schema: PayloadSchema) -> ApiResult<()> {
    schema.updated = Some(Utc::now());
    schema.updated_by = principal.actor().map(str::to_string);
    state.schemas.put(&schema).await?;
    Ok(())
}

#[tracing::instrument(skip_all, fields(handler = "get_schema_tags", schema_id = %id))]
async fn get_tags(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Response> {
    Ok(Json(load(&state, &id).await?.tags).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_schema_tag", schema_id = %id))]
async fn get_tag(
    State(state): State<AppState>,
    Path((id, name)): Path<(String, String)>,
) -> ApiResult<Response> {
    let schema = load(&state, &id).await?;
    match schema.tags.0.get(&name) {
        Some(value) => Ok(Json(value).into_response()),
        None => Err(AppError::NotFound(format!("tag `{name}` not found"))),
    }
}

#[tracing::instrument(skip_all, fields(handler = "put_schema_tag", schema_id = %id))]
async fn put_tag(
    State(state): State<AppState>,
    Path((id, name)): Path<(String, String)>,
    principal: Principal,
    Json(value): Json<TagValue>,
) -> ApiResult<Response> {
    let mut schema = load(&state, &id).await?;
    schema.tags.0.insert(name, value);
    save(&state, &principal, schema).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "delete_schema_tag", schema_id = %id))]
async fn delete_tag(
    State(state): State<AppState>,
    Path((id, name)): Path<(String, String)>,
    principal: Principal,
) -> ApiResult<Response> {
    let mut schema = load(&state, &id).await?;
    schema.tags.0.remove(&name);
    save(&state, &principal, schema).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_schema_description", schema_id = %id))]
async fn get_description(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    Ok(Json(load(&state, &id).await?.description).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "put_schema_description", schema_id = %id))]
async fn put_description(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
    Json(value): Json<String>,
) -> ApiResult<Response> {
    let mut schema = load(&state, &id).await?;
    schema.description = Some(value);
    save(&state, &principal, schema).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "delete_schema_description", schema_id = %id))]
async fn delete_description(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
) -> ApiResult<Response> {
    let mut schema = load(&state, &id).await?;
    schema.description = None;
    save(&state, &principal, schema).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_schema_label", schema_id = %id))]
async fn get_label(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Response> {
    Ok(Json(load(&state, &id).await?.label).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "put_schema_label", schema_id = %id))]
async fn put_label(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
    Json(value): Json<String>,
) -> ApiResult<Response> {
    let mut schema = load(&state, &id).await?;
    schema.label = Some(value);
    save(&state, &principal, schema).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "delete_schema_label", schema_id = %id))]
async fn delete_label(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
) -> ApiResult<Response> {
    let mut schema = load(&state, &id).await?;
    schema.label = None;
    save(&state, &principal, schema).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_schema_external_id", schema_id = %id))]
async fn get_external_id(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    Ok(Json(load(&state, &id).await?.external_id).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "put_schema_external_id", schema_id = %id))]
async fn put_external_id(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
    Json(value): Json<String>,
) -> ApiResult<Response> {
    let mut schema = load(&state, &id).await?;
    schema.external_id = Some(value);
    save(&state, &principal, schema).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "delete_schema_external_id", schema_id = %id))]
async fn delete_external_id(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
) -> ApiResult<Response> {
    let mut schema = load(&state, &id).await?;
    schema.external_id = None;
    save(&state, &principal, schema).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    tads_http::bootstrap();
    let state = AppState::from_env()
        .await
        .map_err(|e| lambda_http::Error::from(e.to_string()))?;
    tads_http::serve(router(state)).await
}
