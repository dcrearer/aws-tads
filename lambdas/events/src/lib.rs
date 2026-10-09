//! Events Lambda library — Track Events (`/tracks/{track_id}/events`), the high-volume core path.
//!
//! `POST` is an upsert accepting a single Event or an array, returning per-event outcomes.
//! Each payload is validated against the Track's Payload Schema; `read_only`, `allow_overlaps`,
//! and `editable_events` are enforced. `GET` lists by timerange/ids with cursor pagination.
//! `DELETE` enqueues an async delete request (202).
//!
//! The router is exposed as a library so it can be driven directly in integration tests
//! (`tower::ServiceExt::oneshot`) against `aws-smithy-mocks`-backed repositories.

use std::collections::HashMap;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header::LOCATION};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use chrono::{Duration, Utc};
use serde::Deserialize;
use tads_domain::{
    DeleteStatus, ErrorBody, EventPostResponse, EventPostResponses, PayloadSchema,
    PayloadValidator, TadsId, TimeRange, Track, TrackDeleteRequest, TrackEvent, TrackEventPost,
    compile_schema, validate_with,
};
use tads_http::error::ApiResult;
use tads_http::{AppError, AppState, PagingHeaders, Principal};
use tads_store::{EventQuery, PageParams, StoreError};

const DELETE_REQUEST_TTL_DAYS: i64 = 7;

/// A POST body is either a single Event or a (non-empty) array of Events.
#[derive(Deserialize)]
#[serde(untagged)]
enum PostBody {
    One(TrackEventPost),
    Many(Vec<TrackEventPost>),
}

/// Build the events router bound to the given application state.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route(
            "/tracks/{track_id}/events",
            get(list_events).post(post_events).delete(delete_events),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            tads_http::poke_on_mutation,
        ))
        .with_state(state)
}

#[tracing::instrument(skip_all, fields(handler = "list_events", track_id = %track_id))]
async fn list_events(
    State(state): State<AppState>,
    Path(track_id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    ensure_track_exists(&state, &track_id).await?;

    let timerange = match q.get("timerange") {
        Some(s) => s.parse::<TimeRange>()?,
        None => TimeRange::eternity(),
    };
    let query = EventQuery {
        timerange,
        ids: q
            .get("ids")
            .map(|v| tads_http::query::parse_csv(v))
            .unwrap_or_default(),
        reverse_order: q
            .get("reverse_order")
            .map(|v| tads_http::query::parse_bool(v))
            .unwrap_or(false),
    };
    let page = tads_http::query::page_params(&q);
    let result = state.events.query(&track_id, &query, &page).await?;

    let headers = PagingHeaders::new(&result, &format!("/tracks/{track_id}/events"))
        .with_reverse_order(query.reverse_order)
        .with_timerange(&query.timerange.to_string())
        .into_header_map();
    Ok((headers, Json(result.items)).into_response())
}

#[tracing::instrument(skip_all, fields(handler = "post_events", track_id = %track_id))]
async fn post_events(
    State(state): State<AppState>,
    Path(track_id): Path<String>,
    Json(body): Json<PostBody>,
) -> ApiResult<Response> {
    let track = state
        .tracks
        .get(&track_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("track {track_id} not found")))?;
    if track.read_only {
        return Err(AppError::Forbidden("track is read_only".into()));
    }
    let schema = load_schema(&state, &track).await?;
    // Compile the Track's Payload Schema ONCE per request and reuse it for every Event in the
    // batch — compiling per event is ~155x more expensive (see crates/*/benches).
    let validator = compile_schema(&schema.schema)?;

    let incoming = match body {
        PostBody::One(e) => vec![e],
        PostBody::Many(v) => v,
    };
    if incoming.is_empty() {
        return Err(AppError::BadRequest("event array must not be empty".into()));
    }

    let now = Utc::now();
    let mut responses = Vec::with_capacity(incoming.len());
    for post in incoming {
        responses.push(process_one(&state, &track, &validator, post, now).await);
    }

    let all_successful = responses
        .iter()
        .all(|r| (200..300).contains(&r.http_status_code));
    Ok(Json(EventPostResponses {
        all_successful,
        responses,
    })
    .into_response())
}

/// Validate and upsert a single Event, returning its per-event response (never fails the batch).
async fn process_one(
    state: &AppState,
    track: &Track,
    validator: &PayloadValidator,
    post: TrackEventPost,
    now: chrono::DateTime<Utc>,
) -> EventPostResponse {
    let id = post.id.unwrap_or_else(TadsId::new_v4);

    // Payload must conform to the Track's Payload Schema (validator compiled once per request).
    if let Err(e) = validate_with(validator, &post.payload) {
        return failure(id, 400, "validation_error", &e.to_string());
    }

    // Enforce allow_overlaps / editable_events against existing Events.
    match overlap_check(state, track, &id, &post.timerange).await {
        Ok(Some(resp)) => return resp,
        Ok(None) => {}
        Err(e) => return failure(id, 500, "internal_error", &e.to_string()),
    }

    let event = TrackEvent {
        id,
        timerange: post.timerange,
        payload: post.payload,
        created: Some(now),
    };
    match state.events.upsert(&track.id.to_string(), event).await {
        Ok(existed) => EventPostResponse {
            http_status_code: if existed { 200 } else { 201 },
            id: Some(id),
            error: None,
        },
        Err(e @ StoreError::PayloadTooLarge { .. }) => {
            failure(id, 413, "payload_too_large", &e.to_string())
        }
        Err(e) => failure(id, 500, "internal_error", &e.to_string()),
    }
}

/// Returns `Some(response)` if the Event must be rejected for overlap/edit reasons, else `None`.
async fn overlap_check(
    state: &AppState,
    track: &Track,
    id: &TadsId,
    timerange: &TimeRange,
) -> Result<Option<EventPostResponse>, AppError> {
    if track.allow_overlaps && track.editable_events {
        return Ok(None); // Nothing to enforce.
    }

    let query = EventQuery {
        timerange: *timerange,
        ids: Vec::new(),
        reverse_order: false,
    };
    let existing = state
        .events
        .query(
            &track.id.to_string(),
            &query,
            &PageParams {
                limit: Some(1000),
                page: None,
            },
        )
        .await?;

    // Compare ids by value (TadsId is Copy + Eq) rather than stringifying per existing Event.
    let edits_existing = existing.items.iter().any(|e| e.id == *id);
    let overlaps_other = existing.items.iter().any(|e| e.id != *id);

    if !track.editable_events && edits_existing {
        return Ok(Some(failure(
            *id,
            403,
            "forbidden",
            "editable_events is false; cannot modify an existing Event",
        )));
    }
    if !track.allow_overlaps && overlaps_other {
        return Ok(Some(failure(
            *id,
            400,
            "bad_request",
            "allow_overlaps is false; Event overlaps an existing Event",
        )));
    }
    Ok(None)
}

#[tracing::instrument(skip_all, fields(handler = "delete_events", track_id = %track_id))]
async fn delete_events(
    State(state): State<AppState>,
    Path(track_id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    principal: Principal,
) -> ApiResult<Response> {
    let track = state
        .tracks
        .get(&track_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("track {track_id} not found")))?;
    if track.read_only {
        return Err(AppError::Forbidden("track is read_only".into()));
    }

    let timerange = match q.get("timerange") {
        Some(s) => s.parse::<TimeRange>()?,
        None => TimeRange::eternity(),
    };

    let now = Utc::now();
    let request = TrackDeleteRequest {
        id: TadsId::new_v4(),
        track_id: track.id,
        timerange_to_delete: if timerange.is_eternity() {
            None
        } else {
            Some(timerange)
        },
        timerange_remaining: None,
        delete_track: false,
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

async fn ensure_track_exists(state: &AppState, track_id: &str) -> ApiResult<()> {
    if state.tracks.get(track_id).await?.is_none() {
        return Err(AppError::NotFound(format!("track {track_id} not found")));
    }
    Ok(())
}

async fn load_schema(state: &AppState, track: &Track) -> ApiResult<PayloadSchema> {
    state
        .schemas
        .get(&track.schema_id.to_string())
        .await?
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "track references schema {} which does not exist",
                track.schema_id
            ))
        })
}

fn failure(id: TadsId, code: u16, kind: &str, summary: &str) -> EventPostResponse {
    EventPostResponse {
        http_status_code: code,
        id: Some(id),
        error: Some(ErrorBody {
            kind: kind.to_string(),
            summary: summary.to_string(),
            traceback: None,
            time: Utc::now(),
        }),
    }
}
