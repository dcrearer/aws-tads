//! Track persistence (`track` table).
//!
//! Row layout: `track_id` (PK), `schema_id`/`entity_id`/`label`/`editable_events` projected for
//! filtering, `tags` jsonb (GIN-indexed), and the authoritative `doc` jsonb (the full `Track`).

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{PgPool, QueryBuilder, Row};
use tads_domain::{TimeRange, Track};
use uuid::Uuid;

use crate::cursor::{Page, PageParams, cursor_str, encode_cursor};
use crate::error::StoreError;
use crate::keys::pg_range;
use crate::outbox;

/// Filters for `GET /tracks`.
#[derive(Debug, Clone, Default)]
pub struct TrackQuery {
    pub timerange: Option<TimeRange>,
    pub label: Option<String>,
    pub schema_id: Option<String>,
    pub entity_ids: Vec<String>,
    pub editable_events: Option<bool>,
    pub tag_matches: Vec<(String, String)>,
    pub tag_exists: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TrackRepo {
    pool: PgPool,
}

impl TrackRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get(&self, id: &str) -> Result<Option<Track>, StoreError> {
        let row = sqlx::query("SELECT doc FROM track WHERE track_id = $1")
            .bind(parse_uuid(id)?)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|r| from_doc(r.get("doc"))).transpose()
    }

    /// Upsert a track. Returns `true` if it already existed (→ 204), `false` if created (→ 201).
    pub async fn put(&self, track: &Track) -> Result<bool, StoreError> {
        let doc = serde_json::to_value(track).map_err(StoreError::serialization)?;
        let tags = serde_json::to_value(&track.tags).map_err(StoreError::serialization)?;

        let mut tx = self.pool.begin().await?;
        let inserted: bool = sqlx::query(
            "INSERT INTO track (track_id, schema_id, entity_id, label, editable_events, \
                read_only, allow_overlaps, created, tags, doc) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (track_id) DO UPDATE SET \
                schema_id = EXCLUDED.schema_id, entity_id = EXCLUDED.entity_id, \
                label = EXCLUDED.label, editable_events = EXCLUDED.editable_events, \
                read_only = EXCLUDED.read_only, allow_overlaps = EXCLUDED.allow_overlaps, \
                created = EXCLUDED.created, tags = EXCLUDED.tags, doc = EXCLUDED.doc \
             RETURNING (xmax = 0) AS inserted",
        )
        .bind(track.id.as_uuid())
        .bind(track.schema_id.as_uuid())
        .bind(&track.timeline_reference.entity_id)
        .bind(track.label.as_deref())
        .bind(track.editable_events)
        .bind(track.read_only)
        .bind(track.allow_overlaps)
        .bind(track.created)
        .bind(&tags)
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
            outbox::RES_TRACK,
            &track.id.to_string(),
            Some(&doc),
            json!({ "track_id": track.id.to_string() }),
        )
        .await?;
        tx.commit().await?;
        Ok(!inserted)
    }

    pub async fn delete(&self, id: &str) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        let removed = sqlx::query("DELETE FROM track WHERE track_id = $1 RETURNING track_id")
            .bind(parse_uuid(id)?)
            .fetch_optional(&mut *tx)
            .await?;
        if removed.is_some() {
            outbox::write(
                &mut tx,
                outbox::OP_REMOVE,
                outbox::RES_TRACK,
                id,
                None,
                json!({ "track_id": id }),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Whether any track references `schema_id` — guards schema deletion (returns 400 if so).
    pub async fn references_schema(&self, schema_id: &str) -> Result<bool, StoreError> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM track WHERE schema_id = $1)")
                .bind(parse_uuid(schema_id)?)
                .fetch_one(&self.pool)
                .await?;
        Ok(exists)
    }

    pub async fn list(
        &self,
        query: &TrackQuery,
        page: &PageParams,
    ) -> Result<Page<Track>, StoreError> {
        let limit = page.effective_limit();
        let cursor = page.cursor()?;

        let mut qb: QueryBuilder<sqlx::Postgres> =
            QueryBuilder::new("SELECT track_id, created, doc FROM track WHERE true");
        if let Some(sid) = &query.schema_id {
            qb.push(" AND schema_id = ").push_bind(parse_uuid(sid)?);
        }
        if !query.entity_ids.is_empty() {
            qb.push(" AND entity_id = ANY(")
                .push_bind(query.entity_ids.clone())
                .push(")");
        }
        if let Some(label) = &query.label {
            qb.push(" AND label = ").push_bind(label.clone());
        }
        if let Some(ee) = query.editable_events {
            qb.push(" AND editable_events = ").push_bind(ee);
        }
        if let Some(tr) = &query.timerange {
            // A track "overlaps" the window if any of its events does (matches the reference).
            qb.push(" AND track_id IN (SELECT track_id FROM track_event WHERE tr && ")
                .push_bind(pg_range(tr))
                .push(")");
        }
        for (k, v) in &query.tag_matches {
            // Tag value is a string or array-of-string; match either form.
            qb.push(" AND (tags->>")
                .push_bind(k.clone())
                .push(" = ")
                .push_bind(v.clone())
                .push(" OR tags->")
                .push_bind(k.clone())
                .push(" @> to_jsonb(")
                .push_bind(v.clone())
                .push("::text))");
        }
        for k in &query.tag_exists {
            qb.push(" AND tags ? ").push_bind(k.clone());
        }
        // Keyset on (created, track_id) descending — newest first.
        if let Some(c) = &cursor {
            qb.push(" AND (created, track_id) < (")
                .push_bind(parse_created(&cursor_str(c, "created")?)?)
                .push(", ")
                .push_bind(parse_uuid(&cursor_str(c, "track_id")?)?)
                .push(")");
        }
        qb.push(" ORDER BY created DESC NULLS LAST, track_id DESC LIMIT ")
            .push_bind(i64::from(limit) + 1);

        let rows = qb.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > limit as usize;
        let mut tracks = Vec::with_capacity(rows.len().min(limit as usize));
        let mut last: Option<(Option<DateTime<Utc>>, Uuid)> = None;
        for row in rows.into_iter().take(limit as usize) {
            last = Some((row.get("created"), row.get("track_id")));
            tracks.push(from_doc(row.get("doc"))?);
        }
        let next_key = if has_more {
            last.map(|(created, id)| {
                encode_cursor(&json!({
                    "created": created.map(|t| t.to_rfc3339()),
                    "track_id": id.to_string(),
                }))
            })
        } else {
            None
        };

        Ok(Page {
            items: tracks,
            next_key,
            limit,
        })
    }
}

fn from_doc(doc: Value) -> Result<Track, StoreError> {
    serde_json::from_value(doc).map_err(|e| StoreError::Malformed(e.to_string()))
}

fn parse_uuid(s: &str) -> Result<Uuid, StoreError> {
    s.parse()
        .map_err(|_| StoreError::Malformed(format!("invalid uuid `{s}`")))
}

fn parse_created(s: &str) -> Result<DateTime<Utc>, StoreError> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| StoreError::Cursor(format!("invalid cursor `created`: {e}")))
}
