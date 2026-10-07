//! Track Event persistence (`track_event` table) — the high-volume core path.
//!
//! Row layout: `(track_id, event_id)` primary key (event id is the identity), projected
//! `start_ns`/`end_ns` + an `int8range tr` for the GiST overlap index, `timerange` text, and the
//! authoritative `doc` jsonb (the `TrackEvent`). Payloads live inline. A payload exceeding
//! `max_payload_bytes` is rejected (413) — large data belongs in TAMS, not TADS.
//!
//! Timerange query: `WHERE track_id = $1 AND tr && <window>` — one GiST index scan returns every
//! Event overlapping the window (including one starting earlier but extending into it). Keyset
//! pagination orders by `(start_ns, event_id)`; `reverse_order` flips it.

use serde_json::{Value, json};
use sqlx::{PgPool, QueryBuilder, Row};
use tads_domain::{TimeRange, TrackEvent};
use uuid::Uuid;

use crate::cursor::{Page, PageParams, cursor_i64, cursor_str, encode_cursor};
use crate::error::StoreError;
use crate::keys::{end_ns, pg_range, start_ns};
use crate::outbox;

/// Filters for `GET /tracks/{id}/events`.
#[derive(Debug, Clone)]
pub struct EventQuery {
    pub timerange: TimeRange,
    pub ids: Vec<String>,
    pub reverse_order: bool,
}

impl Default for EventQuery {
    fn default() -> Self {
        Self {
            timerange: TimeRange::eternity(),
            ids: Vec::new(),
            reverse_order: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventRepo {
    pool: PgPool,
    max_payload_bytes: usize,
}

impl EventRepo {
    pub fn new(pool: PgPool, max_payload_bytes: usize) -> Self {
        Self {
            pool,
            max_payload_bytes,
        }
    }

    /// Query events on a track, filtered by timerange (overlap) and optional id set.
    pub async fn query(
        &self,
        track_id: &str,
        query: &EventQuery,
        page: &PageParams,
    ) -> Result<Page<TrackEvent>, StoreError> {
        let track_uuid = parse_uuid(track_id)?;
        let limit = page.effective_limit();
        let cursor = page.cursor()?;

        let mut qb: QueryBuilder<sqlx::Postgres> =
            QueryBuilder::new("SELECT doc, start_ns, event_id FROM track_event WHERE track_id = ");
        qb.push_bind(track_uuid);
        qb.push(" AND tr && ").push_bind(pg_range(&query.timerange));

        if !query.ids.is_empty() {
            let ids: Vec<Uuid> = query.ids.iter().filter_map(|s| s.parse().ok()).collect();
            qb.push(" AND event_id = ANY(").push_bind(ids).push(")");
        }

        if let Some(c) = &cursor {
            let cs = cursor_i64(c, "start_ns")?;
            let ce = parse_uuid(&cursor_str(c, "event_id")?)?;
            let cmp = if query.reverse_order { " < (" } else { " > (" };
            qb.push(" AND (start_ns, event_id)")
                .push(cmp)
                .push_bind(cs)
                .push(", ")
                .push_bind(ce)
                .push(")");
        }

        qb.push(if query.reverse_order {
            " ORDER BY start_ns DESC, event_id DESC"
        } else {
            " ORDER BY start_ns ASC, event_id ASC"
        });
        // Fetch one extra row to detect whether a further page exists.
        qb.push(" LIMIT ").push_bind(i64::from(limit) + 1);

        let rows = qb.build().fetch_all(&self.pool).await?;

        let mut events = Vec::with_capacity(rows.len().min(limit as usize));
        let mut last: Option<(i64, Uuid)> = None;
        let has_more = rows.len() > limit as usize;
        for row in rows.into_iter().take(limit as usize) {
            let doc: Value = row.get("doc");
            last = Some((row.get("start_ns"), row.get("event_id")));
            events.push(
                serde_json::from_value(doc).map_err(|e| StoreError::Malformed(e.to_string()))?,
            );
        }

        let next_key = if has_more {
            last.map(|(s, e)| encode_cursor(&json!({ "start_ns": s, "event_id": e.to_string() })))
        } else {
            None
        };

        Ok(Page {
            items: events,
            next_key,
            limit,
        })
    }

    /// Upsert a single Event. Returns `true` if an Event with the same id already existed (an
    /// in-place replace → 200), `false` if newly created (201). Because `(track_id, event_id)` is
    /// the key, re-posting at a *different* timerange updates in place (no orphaned key).
    pub async fn upsert(&self, track_id: &str, event: TrackEvent) -> Result<bool, StoreError> {
        let track_uuid = parse_uuid(track_id)?;
        let event_uuid = event.id.as_uuid();

        // Serialize once; this doubles as the item-size guard (API contract carried from DynamoDB).
        let doc = serde_json::to_value(&event).map_err(StoreError::serialization)?;
        let size = serde_json::to_vec(&doc)
            .map_err(StoreError::serialization)?
            .len();
        if size > self.max_payload_bytes {
            return Err(StoreError::PayloadTooLarge {
                size,
                limit: self.max_payload_bytes,
            });
        }

        let mut tx = self.pool.begin().await?;
        let inserted: bool = sqlx::query(
            "INSERT INTO track_event (track_id, event_id, start_ns, end_ns, tr, timerange, doc) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (track_id, event_id) DO UPDATE SET \
                start_ns = EXCLUDED.start_ns, end_ns = EXCLUDED.end_ns, tr = EXCLUDED.tr, \
                timerange = EXCLUDED.timerange, doc = EXCLUDED.doc \
             RETURNING (xmax = 0) AS inserted",
        )
        .bind(track_uuid)
        .bind(event_uuid)
        .bind(start_ns(&event.timerange))
        .bind(end_ns(&event.timerange))
        .bind(pg_range(&event.timerange))
        .bind(event.timerange.to_string())
        .bind(&doc)
        .fetch_one(&mut *tx)
        .await?
        .get("inserted");

        outbox::write(
            &mut tx,
            if inserted {
                outbox::OP_INSERT
            } else {
                outbox::OP_MODIFY
            },
            outbox::RES_EVENT,
            track_id,
            Some(&doc),
            json!({ "track_id": track_id, "event_id": event.id.to_string() }),
        )
        .await?;
        tx.commit().await?;
        Ok(!inserted)
    }

    /// Delete every Event wholly contained within `timerange` (eternity deletes all Events on the
    /// track). Returns the number deleted and records one outbox `remove` per deleted Event.
    pub async fn delete_all_within(
        &self,
        track_id: &str,
        timerange: &TimeRange,
    ) -> Result<usize, StoreError> {
        let track_uuid = parse_uuid(track_id)?;
        let mut tx = self.pool.begin().await?;
        let deleted: Vec<Uuid> = sqlx::query(
            "DELETE FROM track_event WHERE track_id = $1 AND tr <@ $2 RETURNING event_id",
        )
        .bind(track_uuid)
        .bind(pg_range(timerange))
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|r| r.get("event_id"))
        .collect();

        for event_id in &deleted {
            outbox::write(
                &mut tx,
                outbox::OP_REMOVE,
                outbox::RES_EVENT,
                track_id,
                None,
                json!({ "track_id": track_id, "event_id": event_id.to_string() }),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(deleted.len())
    }
}

fn parse_uuid(s: &str) -> Result<Uuid, StoreError> {
    s.parse()
        .map_err(|_| StoreError::Malformed(format!("invalid uuid `{s}`")))
}
