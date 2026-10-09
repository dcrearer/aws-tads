//! Transactional outbox — change-data-capture for Postgres (replaces DynamoDB Streams).
//!
//! Every resource mutation writes an outbox row in the **same transaction** as the data change
//! (so capture is atomic — stronger than Streams). The webhook **relay** Lambda claims undispatched
//! rows, fans them out onto the delivery queue, and marks them dispatched **only after** the sends
//! succeed — all within one transaction holding the row locks, so a crash before/ during the send
//! rolls back and the rows are re-processed (at-least-once). A row carries exactly what the relay
//! needs to build a change: the op, the resource kind, the aggregate id, the new document (for
//! insert/modify), and key fields (for removes).

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

    /// Begin a transaction and claim up to `limit` undispatched rows with `FOR UPDATE SKIP LOCKED`
    /// **without** marking them. The returned transaction holds the row locks (so concurrent relays
    /// skip them); the caller sends the deliveries, then calls [`OutboxRepo::mark_dispatched`] and
    /// commits. If the caller drops the transaction instead (crash / send failure), the rows stay
    /// undispatched and are re-claimed on the next run — mark-after-send, at-least-once.
    pub async fn claim_tx(
        &self,
        limit: i64,
    ) -> Result<(sqlx::Transaction<'_, sqlx::Postgres>, Vec<OutboxRow>), StoreError> {
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(
            "SELECT id, op, resource, aggregate_id, doc, key_fields FROM outbox \
             WHERE dispatched_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED",
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;

        let out = rows
            .into_iter()
            .map(|r| OutboxRow {
                id: r.get("id"),
                op: r.get("op"),
                resource: r.get("resource"),
                aggregate_id: r.get("aggregate_id"),
                doc: r.get("doc"),
                key_fields: r.try_get("key_fields").unwrap_or_else(|_| json!({})),
            })
            .collect();
        Ok((tx, out))
    }

    /// Mark the given rows dispatched on the claim transaction. Call after the deliveries have been
    /// enqueued, then commit the transaction to release the locks and persist the marks.
    pub async fn mark_dispatched(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ids: &[i64],
    ) -> Result<(), StoreError> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query("UPDATE outbox SET dispatched_at = now() WHERE id = ANY($1)")
            .bind(ids)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}
