//! Transactional outbox — change-data-capture for Postgres (replaces DynamoDB Streams).
//!
//! Every resource mutation writes an outbox row in the **same transaction** as the data change
//! (so capture is atomic — stronger than Streams). The `outbox-worker` Lambda drains rows and
//! feeds them to the webhook dispatcher. A row carries exactly what the dispatcher needs to build
//! a change: the op, the resource kind, the aggregate id, the new document (for insert/modify),
//! and key fields (for removes).

use serde_json::{Value, json};
use sqlx::{PgPool, Row};

use crate::error::StoreError;

pub const OP_INSERT: &str = "insert";
pub const OP_MODIFY: &str = "modify";
pub const OP_REMOVE: &str = "remove";

pub const RES_SCHEMA: &str = "schema";
pub const RES_TRACK: &str = "track";
pub const RES_EVENT: &str = "event";

/// Append an outbox row on an existing transaction/connection (same tx as the data write).
pub async fn write(
    conn: &mut sqlx::PgConnection,
    op: &str,
    resource: &str,
    aggregate_id: &str,
    doc: Option<&Value>,
    key_fields: Value,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO outbox (op, resource, aggregate_id, doc, key_fields) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(op)
    .bind(resource)
    .bind(aggregate_id)
    .bind(doc)
    .bind(key_fields)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// A claimed outbox row, shaped for the dispatcher adapter.
#[derive(Debug, Clone)]
pub struct OutboxRow {
    pub id: i64,
    pub op: String,
    pub resource: String,
    pub aggregate_id: String,
    pub doc: Option<Value>,
    pub key_fields: Value,
}

/// Drains the outbox for the worker Lambda.
#[derive(Debug, Clone)]
pub struct OutboxRepo {
    pool: PgPool,
}

impl OutboxRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Atomically claim up to `limit` undispatched rows (marking them dispatched) and return them.
    /// `FOR UPDATE SKIP LOCKED` lets multiple workers run without contending. Mark-on-claim is
    /// at-most-once on a worker crash mid-delivery; webhook delivery has its own retries.
    pub async fn claim_batch(&self, limit: i64) -> Result<Vec<OutboxRow>, StoreError> {
        let rows = sqlx::query(
            "UPDATE outbox SET dispatched_at = now() \
             WHERE id IN ( \
                SELECT id FROM outbox WHERE dispatched_at IS NULL \
                ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED \
             ) \
             RETURNING id, op, resource, aggregate_id, doc, key_fields",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| OutboxRow {
                id: r.get("id"),
                op: r.get("op"),
                resource: r.get("resource"),
                aggregate_id: r.get("aggregate_id"),
                doc: r.get("doc"),
                key_fields: r.try_get("key_fields").unwrap_or_else(|_| json!({})),
            })
            .collect())
    }
}
