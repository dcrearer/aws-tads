//! Track persistence (`tads-tracks` table).
//!
//! Item layout: `track_id` (PK), `schema_id` (GSI PK), `entity_id` (GSI PK, from
//! `timeline_reference.entity_id`), projected `label`/`editable_events` for filtering, and `doc`
//! (the full `Track` as a JSON string).

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue, Select};
use tads_domain::{TimeRange, Track};

use crate::cursor::{Page, PageParams, encode_cursor};
use crate::error::StoreError;

const SCHEMA_ID_INDEX: &str = "schema_id-index";
const ENTITY_ID_INDEX: &str = "entity_id-index";

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
    client: Client,
    table: String,
}

impl TrackRepo {
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }

    pub async fn get(&self, id: &str) -> Result<Option<Track>, StoreError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("track_id", AttributeValue::S(id.to_string()))
            .send()
            .await
            .map_err(StoreError::dynamo)?;
        out.item.map(|item| parse_doc(&item)).transpose()
    }

    /// Upsert a track. Returns `true` if it already existed (→ 204), `false` if created (→ 201).
    pub async fn put(&self, track: &Track) -> Result<bool, StoreError> {
        let item = to_item(track)?;
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
            .key("track_id", AttributeValue::S(id.to_string()))
            .send()
            .await
            .map_err(StoreError::dynamo)?;
        Ok(())
    }

    /// Whether any track references `schema_id` — guards schema deletion (returns 400 if so).
    pub async fn references_schema(&self, schema_id: &str) -> Result<bool, StoreError> {
        let out = self
            .client
            .query()
            .table_name(&self.table)
            .index_name(SCHEMA_ID_INDEX)
            .key_condition_expression("schema_id = :sid")
            .expression_attribute_values(":sid", AttributeValue::S(schema_id.to_string()))
            .select(Select::Count)
            .limit(1)
            .send()
            .await
            .map_err(StoreError::dynamo)?;
        Ok(out.count > 0)
    }

    pub async fn list(
        &self,
        query: &TrackQuery,
        page: &PageParams,
    ) -> Result<Page<Track>, StoreError> {
        let limit = page.effective_limit();
        let start = page.exclusive_start_key()?;

        let (items, next) = if let Some(sid) = &query.schema_id {
            let out = self
                .client
                .query()
                .table_name(&self.table)
                .index_name(SCHEMA_ID_INDEX)
                .key_condition_expression("schema_id = :sid")
                .expression_attribute_values(":sid", AttributeValue::S(sid.clone()))
                .limit(limit)
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(StoreError::dynamo)?;
            (out.items.unwrap_or_default(), out.last_evaluated_key)
        } else if query.entity_ids.len() == 1 {
            let out = self
                .client
                .query()
                .table_name(&self.table)
                .index_name(ENTITY_ID_INDEX)
                .key_condition_expression("entity_id = :eid")
                .expression_attribute_values(":eid", AttributeValue::S(query.entity_ids[0].clone()))
                .limit(limit)
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(StoreError::dynamo)?;
            (out.items.unwrap_or_default(), out.last_evaluated_key)
        } else {
            let out = self
                .client
                .scan()
                .table_name(&self.table)
                .limit(limit)
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(StoreError::dynamo)?;
            (out.items.unwrap_or_default(), out.last_evaluated_key)
        };

        let mut tracks: Vec<Track> = items.iter().map(parse_doc).collect::<Result<_, _>>()?;
        apply_filters(&mut tracks, query);

        let next_key = next.as_ref().map(encode_cursor).transpose()?;
        Ok(Page {
            items: tracks,
            next_key,
            limit,
        })
    }
}

fn to_item(track: &Track) -> Result<HashMap<String, AttributeValue>, StoreError> {
    let doc = serde_json::to_string(track).map_err(|e| StoreError::Serialization(e.to_string()))?;
    let mut item = HashMap::new();
    item.insert(
        "track_id".to_string(),
        AttributeValue::S(track.id.to_string()),
    );
    item.insert(
        "schema_id".to_string(),
        AttributeValue::S(track.schema_id.to_string()),
    );
    item.insert(
        "entity_id".to_string(),
        AttributeValue::S(track.timeline_reference.entity_id.clone()),
    );
    if let Some(label) = &track.label {
        item.insert("label".to_string(), AttributeValue::S(label.clone()));
    }
    item.insert(
        "editable_events".to_string(),
        AttributeValue::Bool(track.editable_events),
    );
    item.insert("doc".to_string(), AttributeValue::S(doc));
    Ok(item)
}

fn parse_doc(item: &HashMap<String, AttributeValue>) -> Result<Track, StoreError> {
    let doc = item
        .get("doc")
        .and_then(|v| v.as_s().ok())
        .ok_or_else(|| StoreError::Malformed("track item missing `doc`".into()))?;
    serde_json::from_str(doc).map_err(|e| StoreError::Malformed(e.to_string()))
}

fn apply_filters(tracks: &mut Vec<Track>, query: &TrackQuery) {
    tracks.retain(|t| {
        if let Some(label) = &query.label
            && t.label.as_deref() != Some(label.as_str())
        {
            return false;
        }
        if let Some(ee) = query.editable_events
            && t.editable_events != ee
        {
            return false;
        }
        if !query.entity_ids.is_empty()
            && !query.entity_ids.contains(&t.timeline_reference.entity_id)
        {
            return false;
        }
        if let Some(tr) = &query.timerange
            && let Some(track_tr) = &t.timerange
            && !tr.overlaps(track_tr)
        {
            return false;
        }
        if !query.tag_matches.iter().all(|(k, v)| t.tags.matches(k, v)) {
            return false;
        }
        if !query.tag_exists.iter().all(|k| t.tags.contains_key(k)) {
            return false;
        }
        true
    });
}
