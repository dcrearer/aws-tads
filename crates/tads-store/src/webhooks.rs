//! Webhook subscription persistence (`webhook` table).
//!
//! Row layout: `webhook_id` (PK), `doc` jsonb (the `WebhookGet`, which by schema never contains
//! `api_key_value`), a `status` column, and a separate `api_key_value` column the dispatcher
//! reads to set the subscriber's auth header. Keeping the secret in its own column keeps it out
//! of GET responses; moving it to Secrets Manager is a hardening follow-up.

use serde_json::{Value, json};
use sqlx::{PgPool, QueryBuilder, Row};
use tads_domain::WebhookGet;
use uuid::Uuid;

use crate::cursor::{Page, PageParams, cursor_str, encode_cursor};
use crate::error::StoreError;

/// Filters for `GET /service/webhooks`.
#[derive(Debug, Clone, Default)]
pub struct WebhookQuery {
    pub tag_matches: Vec<(String, String)>,
    pub tag_exists: Vec<String>,
}

/// A stored webhook plus its write-only API key value — returned only to the dispatcher. Never
/// serialized to API clients.
#[derive(Debug, Clone)]
pub struct StoredWebhook {
    pub webhook: WebhookGet,
    pub api_key_value: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WebhookRepo {
    pool: PgPool,
}

impl WebhookRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Upsert a webhook (used by both POST create and PUT replace).
    pub async fn put(
        &self,
        webhook: &WebhookGet,
        api_key_value: Option<&str>,
    ) -> Result<(), StoreError> {
        self.put_stored(&StoredWebhook {
            webhook: webhook.clone(),
            api_key_value: api_key_value.map(str::to_string),
        })
        .await
    }

    /// Upsert a webhook together with its API key (the dispatcher updates status without losing
    /// the stored secret).
    pub async fn put_stored(&self, stored: &StoredWebhook) -> Result<(), StoreError> {
        let doc = serde_json::to_value(&stored.webhook).map_err(StoreError::serialization)?;
        let status = serde_json::to_value(stored.webhook.status)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default();
        sqlx::query(
            "INSERT INTO webhook (webhook_id, api_key_value, status, doc) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (webhook_id) DO UPDATE SET \
                api_key_value = EXCLUDED.api_key_value, status = EXCLUDED.status, \
                doc = EXCLUDED.doc",
        )
        .bind(stored.webhook.id.as_uuid())
        .bind(stored.api_key_value.as_deref())
        .bind(status)
        .bind(&doc)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get(&self, id: &str) -> Result<Option<WebhookGet>, StoreError> {
        let row = sqlx::query("SELECT doc FROM webhook WHERE webhook_id = $1")
            .bind(parse_uuid(id)?)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|r| from_doc(r.get("doc"))).transpose()
    }

    /// All webhooks with their API keys — used by the dispatcher to match change events.
    pub async fn list_all(&self) -> Result<Vec<StoredWebhook>, StoreError> {
        let rows = sqlx::query("SELECT doc, api_key_value FROM webhook")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|r| {
                Ok(StoredWebhook {
                    webhook: from_doc(r.get("doc"))?,
                    api_key_value: r.get("api_key_value"),
                })
            })
            .collect()
    }

    /// Does a webhook with this id exist? Used to distinguish create (201) from replace (200).
    pub async fn exists(&self, id: &str) -> Result<bool, StoreError> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM webhook WHERE webhook_id = $1)")
                .bind(parse_uuid(id)?)
                .fetch_one(&self.pool)
                .await?;
        Ok(exists)
    }

    pub async fn delete(&self, id: &str) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM webhook WHERE webhook_id = $1")
            .bind(parse_uuid(id)?)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list(
        &self,
        query: &WebhookQuery,
        page: &PageParams,
    ) -> Result<Page<WebhookGet>, StoreError> {
        let limit = page.effective_limit();
        let cursor = page.cursor()?;

        let mut qb: QueryBuilder<sqlx::Postgres> =
            QueryBuilder::new("SELECT webhook_id, doc FROM webhook WHERE true");
        for (k, v) in &query.tag_matches {
            qb.push(" AND (doc->'tags'->>")
                .push_bind(k.clone())
                .push(" = ")
                .push_bind(v.clone())
                .push(" OR doc->'tags'->")
                .push_bind(k.clone())
                .push(" @> to_jsonb(")
                .push_bind(v.clone())
                .push("::text))");
        }
        for k in &query.tag_exists {
            qb.push(" AND doc->'tags' ? ").push_bind(k.clone());
        }
        if let Some(c) = &cursor {
            qb.push(" AND webhook_id > ")
                .push_bind(parse_uuid(&cursor_str(c, "webhook_id")?)?);
        }
        qb.push(" ORDER BY webhook_id ASC LIMIT ")
            .push_bind(i64::from(limit) + 1);

        let rows = qb.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > limit as usize;
        let mut webhooks = Vec::with_capacity(rows.len().min(limit as usize));
        let mut last: Option<Uuid> = None;
        for row in rows.into_iter().take(limit as usize) {
            last = Some(row.get("webhook_id"));
            webhooks.push(from_doc(row.get("doc"))?);
        }
        let next_key = has_more
            .then(|| last.map(|id| encode_cursor(&json!({ "webhook_id": id.to_string() }))))
            .flatten();

        Ok(Page {
            items: webhooks,
            next_key,
            limit,
        })
    }
}

fn from_doc(doc: Value) -> Result<WebhookGet, StoreError> {
    serde_json::from_value(doc).map_err(|e| StoreError::Malformed(e.to_string()))
}

fn parse_uuid(s: &str) -> Result<Uuid, StoreError> {
    s.parse()
        .map_err(|_| StoreError::Malformed(format!("invalid uuid `{s}`")))
}
