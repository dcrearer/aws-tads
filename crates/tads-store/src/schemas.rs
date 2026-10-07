//! Payload Schema persistence (`payload_schema` table).
//!
//! Row layout: `schema_id` (PK), optional unique `external_id`, optional `label`, `tags` jsonb,
//! and the authoritative `doc` jsonb (the full `PayloadSchema`).

use serde_json::{Value, json};
use sqlx::{PgPool, QueryBuilder, Row};
use tads_domain::PayloadSchema;
use uuid::Uuid;

use crate::cursor::{Page, PageParams, cursor_str, encode_cursor};
use crate::error::StoreError;
use crate::outbox;

/// Filters for `GET /schemas`.
#[derive(Debug, Clone, Default)]
pub struct SchemaQuery {
    pub label: Option<String>,
    pub external_id: Option<String>,
    pub tag_matches: Vec<(String, String)>,
    pub tag_exists: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SchemaRepo {
    pool: PgPool,
}

impl SchemaRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get(&self, id: &str) -> Result<Option<PayloadSchema>, StoreError> {
        let row = sqlx::query("SELECT doc FROM payload_schema WHERE schema_id = $1")
            .bind(parse_uuid(id)?)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|r| from_doc(r.get("doc"))).transpose()
    }

    /// Upsert a schema. Returns `true` if it already existed (→ 204), `false` if created (→ 201).
    /// A duplicate `external_id` surfaces as `StoreError::Conflict`.
    pub async fn put(&self, schema: &PayloadSchema) -> Result<bool, StoreError> {
        let doc = serde_json::to_value(schema).map_err(StoreError::serialization)?;
        let tags = serde_json::to_value(&schema.tags).map_err(StoreError::serialization)?;

        let mut tx = self.pool.begin().await?;
        let inserted: bool = sqlx::query(
            "INSERT INTO payload_schema (schema_id, external_id, label, tags, doc) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (schema_id) DO UPDATE SET \
                external_id = EXCLUDED.external_id, label = EXCLUDED.label, \
                tags = EXCLUDED.tags, doc = EXCLUDED.doc \
             RETURNING (xmax = 0) AS inserted",
        )
        .bind(schema.id.as_uuid())
        .bind(schema.external_id.as_deref())
        .bind(schema.label.as_deref())
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
            outbox::RES_SCHEMA,
            &schema.id.to_string(),
            Some(&doc),
            json!({ "schema_id": schema.id.to_string() }),
        )
        .await?;
        tx.commit().await?;
        Ok(!inserted)
    }

    pub async fn delete(&self, id: &str) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        let removed =
            sqlx::query("DELETE FROM payload_schema WHERE schema_id = $1 RETURNING schema_id")
                .bind(parse_uuid(id)?)
                .fetch_optional(&mut *tx)
                .await?;
        if removed.is_some() {
            outbox::write(
                &mut tx,
                outbox::OP_REMOVE,
                outbox::RES_SCHEMA,
                id,
                None,
                json!({ "schema_id": id }),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn list(
        &self,
        query: &SchemaQuery,
        page: &PageParams,
    ) -> Result<Page<PayloadSchema>, StoreError> {
        let limit = page.effective_limit();
        let cursor = page.cursor()?;

        let mut qb: QueryBuilder<sqlx::Postgres> =
            QueryBuilder::new("SELECT schema_id, doc FROM payload_schema WHERE true");
        if let Some(ext) = &query.external_id {
            qb.push(" AND external_id = ").push_bind(ext.clone());
        }
        if let Some(label) = &query.label {
            qb.push(" AND label = ").push_bind(label.clone());
        }
        for (k, v) in &query.tag_matches {
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
        if let Some(c) = &cursor {
            qb.push(" AND schema_id > ")
                .push_bind(parse_uuid(&cursor_str(c, "schema_id")?)?);
        }
        qb.push(" ORDER BY schema_id ASC LIMIT ")
            .push_bind(i64::from(limit) + 1);

        let rows = qb.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > limit as usize;
        let mut schemas = Vec::with_capacity(rows.len().min(limit as usize));
        let mut last: Option<Uuid> = None;
        for row in rows.into_iter().take(limit as usize) {
            last = Some(row.get("schema_id"));
            schemas.push(from_doc(row.get("doc"))?);
        }
        let next_key = has_more
            .then(|| last.map(|id| encode_cursor(&json!({ "schema_id": id.to_string() }))))
            .flatten();

        Ok(Page {
            items: schemas,
            next_key,
            limit,
        })
    }
}

fn from_doc(doc: Value) -> Result<PayloadSchema, StoreError> {
    serde_json::from_value(doc).map_err(|e| StoreError::Malformed(e.to_string()))
}

fn parse_uuid(s: &str) -> Result<Uuid, StoreError> {
    s.parse()
        .map_err(|_| StoreError::Malformed(format!("invalid uuid `{s}`")))
}
