//! Tracks Lambda — Track resources (`/tracks/**`, excluding `/events` which is its own Lambda).
//!
//! Implements list + get + upsert + async delete, plus the granular property sub-resources.
//! `DELETE /tracks/{id}` enqueues a delete request and returns 202. Mutations are rejected on a
//! `read_only` track (except toggling `read_only` itself, so it can be unlocked).

use std::collections::HashMap;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header::LOCATION};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use chrono::{Duration, Utc};
use tads_domain::{DeleteStatus, TadsId, TagValue, TimeRange, Track, TrackDeleteRequest};
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
        .route("/tracks/{track_id}/tags", get(get_tags))
        .route(
            "/tracks/{track_id}/tags/{name}",
            get(get_tag).put(put_tag).delete(delete_tag),
        )
        .route(
            "/tracks/{track_id}/description",
            get(get_description)
                .put(put_description)
                .delete(delete_description),
        )
        .route(
            "/tracks/{track_id}/label",
            get(get_label).put(put_label).delete(delete_label),
        )
        .route(
            "/tracks/{track_id}/read_only",
            get(get_read_only).put(put_read_only),
        )
        .route(
            "/tracks/{track_id}/allow_overlaps",
            get(get_allow_overlaps).put(put_allow_overlaps),
        )
        .route(
            "/tracks/{track_id}/editable_events",
            get(get_editable_events).put(put_editable_events),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            tads_http::poke_on_mutation,
        ))
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
            .map(|v| tads_http::query::parse_entity_ids(v))
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

    // TADS AppNote 0001: a tams:// timeline_reference MUST NOT point at a specific TimeRange.
    tads_domain::validate_timeline_reference(&body.timeline_reference)?;

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

    // Can't disable overlaps on a track that already contains overlapping Events (on create there
    // are none, so this is a no-op). The write path enforces the flag for new Events separately.
    if !body.allow_overlaps && state.events.has_overlaps(&id).await? {
        return Err(AppError::Conflict(
            "cannot set allow_overlaps=false: track already has overlapping Events".into(),
        ));
    }

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

// ---- Granular property sub-resources ---------------------------------------------------------
//
// Each loads the track (404 if absent), mutates one field, bumps `metadata_updated`, and
// re-persists. Mutations are rejected on a read_only track, except toggling read_only itself.

async fn load(state: &AppState, id: &str) -> ApiResult<Track> {
    state
        .tracks
        .get(id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("track {id} not found")))
}

/// Persist a metadata mutation. `allow_read_only` permits writing a read_only track (used only by
/// the read_only toggle, so a locked track can be unlocked).
async fn save(
    state: &AppState,
    principal: &Principal,
    mut track: Track,
    was_read_only: bool,
    allow_read_only: bool,
) -> ApiResult<()> {
    if was_read_only && !allow_read_only {
        return Err(AppError::Forbidden("track is read_only".into()));
    }
    track.metadata_updated = Some(Utc::now());
    track.updated_by = principal.actor().map(str::to_string);
    state.tracks.put(&track).await?;
    Ok(())
}

#[tracing::instrument(skip_all, fields(handler = "get_track_tags", track_id = %id))]
async fn get_tags(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Response> {
    Ok(Json(load(&state, &id).await?.tags).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_track_tag", track_id = %id))]
async fn get_tag(
    State(state): State<AppState>,
    Path((id, name)): Path<(String, String)>,
) -> ApiResult<Response> {
    let track = load(&state, &id).await?;
    match track.tags.0.get(&name) {
        Some(value) => Ok(Json(value).into_response()),
        None => Err(AppError::NotFound(format!("tag `{name}` not found"))),
    }
}

#[tracing::instrument(skip_all, fields(handler = "put_track_tag", track_id = %id))]
async fn put_tag(
    State(state): State<AppState>,
    Path((id, name)): Path<(String, String)>,
    principal: Principal,
    Json(value): Json<TagValue>,
) -> ApiResult<Response> {
    let mut track = load(&state, &id).await?;
    let ro = track.read_only;
    track.tags.0.insert(name, value);
    save(&state, &principal, track, ro, false).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "delete_track_tag", track_id = %id))]
async fn delete_tag(
    State(state): State<AppState>,
    Path((id, name)): Path<(String, String)>,
    principal: Principal,
) -> ApiResult<Response> {
    let mut track = load(&state, &id).await?;
    let ro = track.read_only;
    track.tags.0.remove(&name);
    save(&state, &principal, track, ro, false).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_track_description", track_id = %id))]
async fn get_description(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    Ok(Json(load(&state, &id).await?.description).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "put_track_description", track_id = %id))]
async fn put_description(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
    Json(value): Json<String>,
) -> ApiResult<Response> {
    let mut track = load(&state, &id).await?;
    let ro = track.read_only;
    track.description = Some(value);
    save(&state, &principal, track, ro, false).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "delete_track_description", track_id = %id))]
async fn delete_description(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
) -> ApiResult<Response> {
    let mut track = load(&state, &id).await?;
    let ro = track.read_only;
    track.description = None;
    save(&state, &principal, track, ro, false).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_track_label", track_id = %id))]
async fn get_label(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Response> {
    Ok(Json(load(&state, &id).await?.label).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "put_track_label", track_id = %id))]
async fn put_label(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
    Json(value): Json<String>,
) -> ApiResult<Response> {
    let mut track = load(&state, &id).await?;
    let ro = track.read_only;
    track.label = Some(value);
    save(&state, &principal, track, ro, false).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "delete_track_label", track_id = %id))]
async fn delete_label(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
) -> ApiResult<Response> {
    let mut track = load(&state, &id).await?;
    let ro = track.read_only;
    track.label = None;
    save(&state, &principal, track, ro, false).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_track_read_only", track_id = %id))]
async fn get_read_only(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    Ok(Json(load(&state, &id).await?.read_only).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "put_track_read_only", track_id = %id))]
async fn put_read_only(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
    Json(value): Json<bool>,
) -> ApiResult<Response> {
    let mut track = load(&state, &id).await?;
    track.read_only = value;
    // The read_only toggle may always be written (so a locked track can be unlocked).
    save(&state, &principal, track, false, true).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_track_allow_overlaps", track_id = %id))]
async fn get_allow_overlaps(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    Ok(Json(load(&state, &id).await?.allow_overlaps).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "put_track_allow_overlaps", track_id = %id))]
async fn put_allow_overlaps(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
    Json(value): Json<bool>,
) -> ApiResult<Response> {
    let mut track = load(&state, &id).await?;
    let ro = track.read_only;
    // Disabling overlaps is only allowed when no two Events currently overlap on this track.
    if !value && state.events.has_overlaps(&id).await? {
        return Err(AppError::Conflict(
            "cannot set allow_overlaps=false: track already has overlapping Events".into(),
        ));
    }
    track.allow_overlaps = value;
    save(&state, &principal, track, ro, false).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[tracing::instrument(skip_all, fields(handler = "get_track_editable_events", track_id = %id))]
async fn get_editable_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    Ok(Json(load(&state, &id).await?.editable_events).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "put_track_editable_events", track_id = %id))]
async fn put_editable_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    principal: Principal,
    Json(value): Json<bool>,
) -> ApiResult<Response> {
    let mut track = load(&state, &id).await?;
    let ro = track.read_only;
    track.editable_events = value;
    save(&state, &principal, track, ro, false).await?;
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
