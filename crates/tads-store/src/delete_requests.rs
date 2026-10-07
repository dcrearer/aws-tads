//! Track Delete Request persistence (`track_delete_request` table) + SQS enqueue.
//!
//! A `DELETE /tracks/{id}` (or bulk event delete) creates a request here (status `created`),
//! enqueues an SQS message, and returns 202 + `Location`. The delete-worker consumes the queue
//! and advances the status. `expiry` is epoch seconds, swept by the worker (no DynamoDB TTL now).

use aws_sdk_sqs::Client as SqsClient;
use serde_json::{Value, json};
use sqlx::{PgPool, QueryBuilder, Row};
use tads_domain::TrackDeleteRequest;
use uuid::Uuid;

use crate::cursor::{Page, PageParams, cursor_str, encode_cursor};
use crate::error::StoreError;

/// Sort field for `GET /track-delete-requests`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteSortBy {
    Created,
    Expiry,
}

#[derive(Debug, Clone)]
pub struct DeleteRequestRepo {
    pool: PgPool,
    sqs: SqsClient,
    queue_url: String,
}

impl DeleteRequestRepo {
    pub fn new(pool: PgPool, sqs: SqsClient, queue_url: impl Into<String>) -> Self {
        Self {
            pool,
            sqs,
            queue_url: queue_url.into(),
        }
    }

    /// Persist a new delete request and enqueue it for the worker.
    pub async fn create(&self, request: &TrackDeleteRequest) -> Result<(), StoreError> {
        self.put(request).await?;
        self.sqs
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(request.id.to_string())
            .send()
            .await
            .map_err(StoreError::backend)?;
        Ok(())
    }

    /// Persist status/progress updates (delete-worker advances `created → started → done|error`).
    pub async fn update(&self, request: &TrackDeleteRequest) -> Result<(), StoreError> {
        self.put(request).await
    }

    async fn put(&self, request: &TrackDeleteRequest) -> Result<(), StoreError> {
        let doc = serde_json::to_value(request).map_err(StoreError::serialization)?;
        let status = serde_json::to_value(request.status)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default();
        sqlx::query(
            "INSERT INTO track_delete_request (request_id, status, expiry, created, doc) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (request_id) DO UPDATE SET \
                status = EXCLUDED.status, expiry = EXCLUDED.expiry, doc = EXCLUDED.doc",
        )
        .bind(request.id.as_uuid())
        .bind(status)
        .bind(request.expiry.map(|e| e.timestamp()))
        .bind(request.created)
        .bind(&doc)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get(&self, id: &str) -> Result<Option<TrackDeleteRequest>, StoreError> {
        let row = sqlx::query("SELECT doc FROM track_delete_request WHERE request_id = $1")
            .bind(parse_uuid(id)?)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|r| from_doc(r.get("doc"))).transpose()
    }

    /// List delete requests. The spec permits an incomplete/unordered listing; we keyset-paginate
    /// on `request_id` for stability (`sort_by`/`reverse_order` are not yet applied).
    pub async fn list(
        &self,
        _sort_by: DeleteSortBy,
        _reverse_order: bool,
        page: &PageParams,
    ) -> Result<Page<TrackDeleteRequest>, StoreError> {
        let limit = page.effective_limit();
        let cursor = page.cursor()?;

        let mut qb: QueryBuilder<sqlx::Postgres> =
            QueryBuilder::new("SELECT request_id, doc FROM track_delete_request WHERE true");
        if let Some(c) = &cursor {
            qb.push(" AND request_id > ")
                .push_bind(parse_uuid(&cursor_str(c, "request_id")?)?);
        }
        qb.push(" ORDER BY request_id ASC LIMIT ")
            .push_bind(i64::from(limit) + 1);

        let rows = qb.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > limit as usize;
        let mut items = Vec::with_capacity(rows.len().min(limit as usize));
        let mut last: Option<Uuid> = None;
        for row in rows.into_iter().take(limit as usize) {
            last = Some(row.get("request_id"));
            items.push(from_doc(row.get("doc"))?);
        }
        let next_key = has_more
            .then(|| last.map(|id| encode_cursor(&json!({ "request_id": id.to_string() }))))
            .flatten();

        Ok(Page {
            items,
            next_key,
            limit,
        })
    }
}

fn from_doc(doc: Value) -> Result<TrackDeleteRequest, StoreError> {
    serde_json::from_value(doc).map_err(|e| StoreError::Malformed(e.to_string()))
}

fn parse_uuid(s: &str) -> Result<Uuid, StoreError> {
    s.parse()
        .map_err(|_| StoreError::Malformed(format!("invalid uuid `{s}`")))
}
