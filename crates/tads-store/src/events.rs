//! Track Event persistence (`tads-events` table) — the high-volume core path.
//!
//! Item layout: `track_id` (PK), `sk` = `<sortable(start_ns)>#<event_id>` (range key),
//! projected `start_ns`/`end_ns` (numbers) for timerange queries, `timerange`/`event_id`, and
//! `doc` (the `TrackEvent` JSON). Payloads live **inline** in `doc`; a payload approaching the
//! item-size limit is offloaded to S3 (`s3_key`) and transparently rehydrated on read.
//!
//! Timerange query: `track_id = :tid AND sk < :hi` (start strictly below the exclusive upper
//! bound) with a `end_ns > :lo` filter — this returns every Event overlapping the window,
//! including one that starts earlier but extends into it. `reverse_order` flips the scan order.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use serde_json::Value;
use tads_domain::{TimeRange, TrackEvent};

use crate::cursor::{Page, PageParams, encode_cursor};
use crate::error::StoreError;
use crate::keys::{event_sort_key, start_upper_bound};

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
    dynamo: DynamoClient,
    s3: S3Client,
    table: String,
    bucket: String,
    overflow_threshold: usize,
}

impl EventRepo {
    pub fn new(
        dynamo: DynamoClient,
        s3: S3Client,
        table: impl Into<String>,
        bucket: impl Into<String>,
        overflow_threshold: usize,
    ) -> Self {
        Self {
            dynamo,
            s3,
            table: table.into(),
            bucket: bucket.into(),
            overflow_threshold,
        }
    }

    /// Query events on a track, filtered by timerange (overlap) and optional id set.
    pub async fn query(
        &self,
        track_id: &str,
        query: &EventQuery,
        page: &PageParams,
    ) -> Result<Page<TrackEvent>, StoreError> {
        let limit = page.effective_limit();
        let start = page.exclusive_start_key()?;

        let lo = query.timerange.lower_nanos();
        let hi = query.timerange.upper_nanos();

        let out = self
            .dynamo
            .query()
            .table_name(&self.table)
            .key_condition_expression("track_id = :tid AND sk < :hi")
            .expression_attribute_values(":tid", AttributeValue::S(track_id.to_string()))
            .expression_attribute_values(":hi", AttributeValue::S(start_upper_bound(hi)))
            .filter_expression("end_ns > :lo")
            .expression_attribute_values(":lo", AttributeValue::N(lo.to_string()))
            .scan_index_forward(!query.reverse_order)
            .limit(limit)
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(StoreError::dynamo)?;

        let items = out.items.unwrap_or_default();
        let mut events = Vec::with_capacity(items.len());
        for item in &items {
            let event = self.parse_event(item).await?;
            if query.ids.is_empty() || query.ids.contains(&event.id.to_string()) {
                events.push(event);
            }
        }

        let next_key = out
            .last_evaluated_key
            .as_ref()
            .map(encode_cursor)
            .transpose()?;
        Ok(Page {
            items: events,
            next_key,
            limit,
        })
    }

    /// Upsert a single Event. Returns `true` if an item with the same sort key already existed
    /// (same id + same start instant → an in-place replace / 200), `false` if newly created (201).
    ///
    /// Note: moving an Event to a *different* timerange changes its sort key; cleaning up the
    /// prior key requires an event-id GSI and is handled in Phase 2. Re-posting at the same
    /// timerange (correcting a payload) replaces in place.
    pub async fn upsert(&self, track_id: &str, mut event: TrackEvent) -> Result<bool, StoreError> {
        let start_ns = event.timerange.lower_nanos();
        let end_ns = event.timerange.upper_nanos();
        let event_id = event.id.to_string();
        let sk = event_sort_key(start_ns, &event_id);

        // Serialize the payload once. These bytes are the overflow unit of measure and, when the
        // payload is offloaded, are reused as the S3 body — so a large payload is never cloned
        // and never serialized twice on the overflow path.
        let payload_bytes = serde_json::to_vec(&event.payload)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let overflow = payload_bytes.len() > self.overflow_threshold;

        let mut item = HashMap::new();
        item.insert(
            "track_id".to_string(),
            AttributeValue::S(track_id.to_string()),
        );
        item.insert("sk".to_string(), AttributeValue::S(sk));
        item.insert("event_id".to_string(), AttributeValue::S(event_id));
        item.insert(
            "start_ns".to_string(),
            AttributeValue::N(start_ns.to_string()),
        );
        item.insert("end_ns".to_string(), AttributeValue::N(end_ns.to_string()));
        item.insert(
            "timerange".to_string(),
            AttributeValue::S(event.timerange.to_string()),
        );

        if overflow {
            // Offload the payload to S3; store the Event with a null payload + pointer. Moving
            // the payload out (not cloning) leaves `event` as the shell we serialize next.
            let s3_key = format!("{track_id}/{}", event.id);
            self.s3
                .put_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .body(ByteStream::from(payload_bytes))
                .content_type("application/json")
                .send()
                .await
                .map_err(StoreError::s3)?;

            event.payload = Value::Null;
            let doc_shell = serde_json::to_string(&event)
                .map_err(|e| StoreError::Serialization(e.to_string()))?;
            item.insert("doc".to_string(), AttributeValue::S(doc_shell));
            item.insert("s3_key".to_string(), AttributeValue::S(s3_key));
        } else {
            let doc_full = serde_json::to_string(&event)
                .map_err(|e| StoreError::Serialization(e.to_string()))?;
            item.insert("doc".to_string(), AttributeValue::S(doc_full));
        }

        let out = self
            .dynamo
            .put_item()
            .table_name(&self.table)
            .set_item(Some(item))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await
            .map_err(StoreError::dynamo)?;
        Ok(out.attributes.is_some())
    }

    async fn parse_event(
        &self,
        item: &HashMap<String, AttributeValue>,
    ) -> Result<TrackEvent, StoreError> {
        let doc = item
            .get("doc")
            .and_then(|v| v.as_s().ok())
            .ok_or_else(|| StoreError::Malformed("event item missing `doc`".into()))?;
        let mut event: TrackEvent =
            serde_json::from_str(doc).map_err(|e| StoreError::Malformed(e.to_string()))?;

        // Rehydrate an overflowed payload from S3.
        if let Some(key) = item.get("s3_key").and_then(|v| v.as_s().ok()) {
            let obj = self
                .s3
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(StoreError::s3)?;
            let bytes = obj
                .body
                .collect()
                .await
                .map_err(StoreError::s3)?
                .into_bytes();
            event.payload =
                serde_json::from_slice(&bytes).map_err(|e| StoreError::Malformed(e.to_string()))?;
        }
        Ok(event)
    }
}
