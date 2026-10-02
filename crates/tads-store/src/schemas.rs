//! Payload Schema persistence (`tads-schemas` table).
//!
//! Item layout: `schema_id` (PK), optional `external_id` (GSI PK), optional `label`, and `doc`
//! (the full `PayloadSchema` as a JSON string). Projected attributes support the spec's list
//! filters; the authoritative object is always `doc`.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use tads_domain::PayloadSchema;

use crate::cursor::{Page, PageParams, encode_cursor};
use crate::error::StoreError;

const EXTERNAL_ID_INDEX: &str = "external_id-index";

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
    client: Client,
    table: String,
}

impl SchemaRepo {
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }

    pub async fn get(&self, id: &str) -> Result<Option<PayloadSchema>, StoreError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("schema_id", AttributeValue::S(id.to_string()))
            .send()
            .await
            .map_err(StoreError::dynamo)?;

        out.item.map(|item| parse_doc(&item)).transpose()
    }

    /// Upsert a schema. Returns `true` if a schema with this id already existed (→ HTTP 204),
    /// `false` if newly created (→ HTTP 201).
    pub async fn put(&self, schema: &PayloadSchema) -> Result<bool, StoreError> {
        let item = to_item(schema)?;
        let out = self
            .client
            .put_item()
            .table_name(&self.table)
            .set_item(Some(item))
            .return_values(ReturnValue::AllOld)
            .send()
            .await
            .map_err(StoreError::dynamo)?;
        Ok(out.attributes.is_some())
    }

    pub async fn delete(&self, id: &str) -> Result<(), StoreError> {
        self.client
            .delete_item()
            .table_name(&self.table)
            .key("schema_id", AttributeValue::S(id.to_string()))
            .send()
            .await
            .map_err(StoreError::dynamo)?;
        Ok(())
    }

    pub async fn list(
        &self,
        query: &SchemaQuery,
        page: &PageParams,
    ) -> Result<Page<PayloadSchema>, StoreError> {
        let limit = page.effective_limit();
        let start = page.exclusive_start_key()?;

        // external_id is a GSI; everything else is a scan with server-side label filtering plus
        // in-memory tag filtering of the page.
        let (items, next) = if let Some(ext) = &query.external_id {
            let out = self
                .client
                .query()
                .table_name(&self.table)
                .index_name(EXTERNAL_ID_INDEX)
                .key_condition_expression("external_id = :ext")
                .expression_attribute_values(":ext", AttributeValue::S(ext.clone()))
                .limit(limit)
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(StoreError::dynamo)?;
            (out.items.unwrap_or_default(), out.last_evaluated_key)
        } else {
            let mut scan = self
                .client
                .scan()
                .table_name(&self.table)
                .limit(limit)
                .set_exclusive_start_key(start);
            if let Some(label) = &query.label {
                scan = scan
                    .filter_expression("#l = :label")
                    .expression_attribute_names("#l", "label")
                    .expression_attribute_values(":label", AttributeValue::S(label.clone()));
            }
            let out = scan.send().await.map_err(StoreError::dynamo)?;
            (out.items.unwrap_or_default(), out.last_evaluated_key)
        };

        let mut schemas: Vec<PayloadSchema> =
            items.iter().map(parse_doc).collect::<Result<_, _>>()?;
        apply_tag_filters(&mut schemas, query);

        let next_key = next.as_ref().map(encode_cursor).transpose()?;
        Ok(Page {
            items: schemas,
            next_key,
            limit,
        })
    }
}

fn to_item(schema: &PayloadSchema) -> Result<HashMap<String, AttributeValue>, StoreError> {
    let doc =
        serde_json::to_string(schema).map_err(|e| StoreError::Serialization(e.to_string()))?;
    let mut item = HashMap::new();
    item.insert(
        "schema_id".to_string(),
        AttributeValue::S(schema.id.to_string()),
    );
    if let Some(ext) = &schema.external_id {
        item.insert("external_id".to_string(), AttributeValue::S(ext.clone()));
    }
    if let Some(label) = &schema.label {
        item.insert("label".to_string(), AttributeValue::S(label.clone()));
    }
    item.insert("doc".to_string(), AttributeValue::S(doc));
    Ok(item)
}

fn parse_doc(item: &HashMap<String, AttributeValue>) -> Result<PayloadSchema, StoreError> {
    let doc = item
        .get("doc")
        .and_then(|v| v.as_s().ok())
        .ok_or_else(|| StoreError::Malformed("schema item missing `doc`".into()))?;
    serde_json::from_str(doc).map_err(|e| StoreError::Malformed(e.to_string()))
}

fn apply_tag_filters(schemas: &mut Vec<PayloadSchema>, query: &SchemaQuery) {
    if query.tag_matches.is_empty() && query.tag_exists.is_empty() {
        return;
    }
    schemas.retain(|s| {
        query.tag_matches.iter().all(|(k, v)| s.tags.matches(k, v))
            && query.tag_exists.iter().all(|k| s.tags.contains_key(k))
    });
}
