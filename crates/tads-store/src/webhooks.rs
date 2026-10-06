//! Webhook subscription persistence (`tads-webhooks` table).
//!
//! Item layout: `webhook_id` (PK), `doc` (the `WebhookGet` JSON — which by schema never contains
//! `api_key_value`), and a separate `api_key_value` attribute the dispatcher reads to set the
//! subscriber's auth header. Storing the secret in a dedicated attribute keeps it out of GET
//! responses; moving it to Secrets Manager is a hardening follow-up.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use tads_domain::WebhookGet;

use crate::cursor::{Page, PageParams, encode_cursor};
use crate::error::StoreError;

/// Filters for `GET /service/webhooks`.
#[derive(Debug, Clone, Default)]
pub struct WebhookQuery {
    pub tag_matches: Vec<(String, String)>,
    pub tag_exists: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct WebhookRepo {
    client: Client,
    table: String,
}

impl WebhookRepo {
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }

    /// Upsert a webhook (used by both POST create and PUT replace).
    pub async fn put(
        &self,
        webhook: &WebhookGet,
        api_key_value: Option<&str>,
    ) -> Result<(), StoreError> {
        let doc =
            serde_json::to_string(webhook).map_err(|e| StoreError::Serialization(e.to_string()))?;
        let mut item = HashMap::new();
        item.insert(
            "webhook_id".to_string(),
            AttributeValue::S(webhook.id.to_string()),
        );
        item.insert("doc".to_string(), AttributeValue::S(doc));
        if let Some(key) = api_key_value {
            item.insert(
                "api_key_value".to_string(),
                AttributeValue::S(key.to_string()),
            );
        }
        self.client
            .put_item()
            .table_name(&self.table)
            .set_item(Some(item))
            .send()
            .await
            .map_err(StoreError::dynamo)?;
        Ok(())
    }

    pub async fn get(&self, id: &str) -> Result<Option<WebhookGet>, StoreError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("webhook_id", AttributeValue::S(id.to_string()))
            .send()
            .await
            .map_err(StoreError::dynamo)?;
        out.item.map(|item| parse_doc(&item)).transpose()
    }

    /// Does a webhook with this id exist? Used to distinguish create (201) from replace (200).
    pub async fn exists(&self, id: &str) -> Result<bool, StoreError> {
        Ok(self.get(id).await?.is_some())
    }

    pub async fn delete(&self, id: &str) -> Result<(), StoreError> {
        self.client
            .delete_item()
            .table_name(&self.table)
            .key("webhook_id", AttributeValue::S(id.to_string()))
            .send()
            .await
            .map_err(StoreError::dynamo)?;
        Ok(())
    }

    pub async fn list(
        &self,
        query: &WebhookQuery,
        page: &PageParams,
    ) -> Result<Page<WebhookGet>, StoreError> {
        let limit = page.effective_limit();
        let start = page.exclusive_start_key()?;
        let out = self
            .client
            .scan()
            .table_name(&self.table)
            .limit(limit)
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(StoreError::dynamo)?;

        let mut webhooks: Vec<WebhookGet> = out
            .items
            .unwrap_or_default()
            .iter()
            .map(parse_doc)
            .collect::<Result<_, _>>()?;
        apply_tag_filters(&mut webhooks, query);

        let next_key = out
            .last_evaluated_key
            .as_ref()
            .map(encode_cursor)
            .transpose()?;
        Ok(Page {
            items: webhooks,
            next_key,
            limit,
        })
    }
}

fn parse_doc(item: &HashMap<String, AttributeValue>) -> Result<WebhookGet, StoreError> {
    let doc = item
        .get("doc")
        .and_then(|v| v.as_s().ok())
        .ok_or_else(|| StoreError::Malformed("webhook item missing `doc`".into()))?;
    serde_json::from_str(doc).map_err(|e| StoreError::Malformed(e.to_string()))
}

fn apply_tag_filters(webhooks: &mut Vec<WebhookGet>, query: &WebhookQuery) {
    if query.tag_matches.is_empty() && query.tag_exists.is_empty() {
        return;
    }
    webhooks.retain(|w| {
        query
            .tag_matches
            .iter()
            .all(|(k, v)| w.webhook.tags.matches(k, v))
            && query
                .tag_exists
                .iter()
                .all(|k| w.webhook.tags.contains_key(k))
    });
}
