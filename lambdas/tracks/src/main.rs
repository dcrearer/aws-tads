//! Tracks Lambda — Track resources (`/tracks/**`, excluding `/events` which is its own Lambda).
//!
//! Phase 1 implements list + get + upsert + async delete. Granular property sub-resources are
//! 501 stubs (Phase 2). `DELETE /tracks/{id}` enqueues a delete request and returns 202.

use std::collections::HashMap;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header::LOCATION};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use chrono::{Duration, Utc};
use tads_domain::{DeleteStatus, TadsId, TimeRange, Track, TrackDeleteRequest};
use tads_http::error::ApiResult;
use tads_http::{AppError, AppState, PagingHeaders, Principal};
use tads_store::TrackQuery;

/// How long a completed delete-request record is retained (DynamoDB TTL).
const DELETE_REQUEST_TTL_DAYS: i64 = 7;

fn router(state: AppState) -> Router {
    Router::new()
        .route("/tracks", get(list_tracks))
        .route(
            "/tracks/{track_id}",
            get(get_track).put(put_track).delete(delete_track),
        )
        // Granular property sub-resources — Phase 2.
        .route("/tracks/{track_id}/tags", get(todo_501))
        .route(
            "/tracks/{track_id}/tags/{name}",
            get(todo_501).put(todo_501).delete(todo_501),
        )
        .route(
            "/tracks/{track_id}/description",
            get(todo_501).put(todo_501).delete(todo_501),
        )
        .route(
            "/tracks/{track_id}/label",
            get(todo_501).put(todo_501).delete(todo_501),
        )
        .route("/tracks/{track_id}/read_only", get(todo_501).put(todo_501))
        .route(
            "/tracks/{track_id}/allow_overlaps",
            get(todo_501).put(todo_501),
        )
        .route(
            "/tracks/{track_id}/editable_events",
            get(todo_501).put(todo_501),
        )
        .with_state(state)
}

#[tracing::instrument(skip_all, fields(handler = "list_tracks"))]
async fn list_tracks(
    State(state): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let timerange = match q.get("timerange") {
        Some(s) => Some(s.parse::<TimeRange>()?),
        None => None,
    };
    let (tag_matches, tag_exists) = tads_http::query::tag_filters(&q);
    let query = TrackQuery {
        timerange,
        label: q.get("label").cloned(),
        schema_id: q.get("schema_id").cloned(),
        entity_ids: q
            .get("timeline_reference.entity_id")
            .map(|v| tads_http::query::parse_csv(v))
            .unwrap_or_default(),
        editable_events: q
            .get("editable_events")
            .map(|v| tads_http::query::parse_bool(v)),
        tag_matches,
        tag_exists,
    };
    let page = tads_http::query::page_params(&q);
    let result = state.tracks.list(&query, &page).await?;

    let headers = PagingHeaders::new(&result, "/tracks").into_header_map();
    Ok((headers, Json(result.items)).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_track", track_id = %id))]
async fn get_track(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Response> {
    match state.tracks.get(&id).await? {
        Some(track) => Ok(Json(track).into_response()),
        None => Err(AppError::NotFound(format!("track {id} not found"))),
    }
}

#[tracing::instrument(skip_all, fields(handler = "put_track", track_id = %id))]
async fn put_track(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
    Json(mut body): Json<Track>,
) -> ApiResult<Response> {
    let tid: TadsId = id
        .parse()
        .map_err(|_| AppError::BadRequest(format!("invalid track id `{id}`")))?;
    body.id = tid;

    let now = Utc::now();
    // Server manages these; ignore any client-supplied values.
    body.events_updated = None;
    body.timerange = None;

    let existed = match state.tracks.get(&id).await? {
        Some(existing) => {
            if existing.read_only {
                return Err(AppError::Forbidden("track is read_only".into()));
            }
            body.created = existing.created;
            body.created_by = existing.created_by;
            body.events_updated = existing.events_updated;
            true
        }
        None => {
            body.created = Some(now);
            body.created_by = principal.actor().map(str::to_string);
            false
        }
    };
    body.metadata_updated = Some(now);
    body.updated_by = principal.actor().map(str::to_string);

    state.tracks.put(&body).await?;

    if existed {
        Ok(StatusCode::NO_CONTENT.into_response())
    } else {
        Ok((StatusCode::CREATED, Json(body)).into_response())
    }
}

#[tracing::instrument(skip_all, fields(handler = "delete_track", track_id = %id))]
async fn delete_track(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
) -> ApiResult<Response> {
    let track = state
        .tracks
        .get(&id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("track {id} not found")))?;
    if track.read_only {
        return Err(AppError::Forbidden("track is read_only".into()));
    }

    let now = Utc::now();
    let request = TrackDeleteRequest {
        id: TadsId::new_v4(),
        track_id: track.id,
        timerange_to_delete: None,
        timerange_remaining: None,
        delete_track: true,
        created: Some(now),
        created_by: principal.actor().map(str::to_string),
        updated: Some(now),
        expiry: Some(now + Duration::days(DELETE_REQUEST_TTL_DAYS)),
        status: DeleteStatus::Created,
        error: None,
    };
    state.delete_requests.create(&request).await?;

    let location = format!("/track-delete-requests/{}", request.id);
    let mut headers = HeaderMap::new();
    if let Ok(v) = location.parse() {
        headers.insert(LOCATION, v);
    }
    Ok((StatusCode::ACCEPTED, headers, Json(request)).into_response())
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
