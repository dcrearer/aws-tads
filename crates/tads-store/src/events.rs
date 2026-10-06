//! Track Event persistence (`tads-events` table) — the high-volume core path.
//!
//! Item layout: `track_id` (PK), `sk` = `<sortable(start_ns)>#<event_id>` (range key),
//! projected `start_ns`/`end_ns` (numbers) for timerange queries, `timerange`/`event_id`, and
//! `doc` (the `TrackEvent` JSON). Payloads live **inline** in `doc`. TADS stores small metadata,
//! so a payload exceeding `max_payload_bytes` (held under DynamoDB's 400 KB item limit) is a
//! client error — such data belongs in TAMS, not TADS — and is rejected, not offloaded.
//!
//! Timerange query: `track_id = :tid AND sk < :hi` (start strictly below the exclusive upper
//! bound) with a `end_ns > :lo` filter — this returns every Event overlapping the window,
//! including one that starts earlier but extends into it. `reverse_order` flips the scan order.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::types::AttributeValue;
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
    table: String,
    max_payload_bytes: usize,
}

impl EventRepo {
    pub fn new(dynamo: DynamoClient, table: impl Into<String>, max_payload_bytes: usize) -> Self {
        Self {
            dynamo,
            table: table.into(),
            max_payload_bytes,
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
            let event = Self::parse_event(item)?;
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
    pub async fn upsert(&self, track_id: &str, event: TrackEvent) -> Result<bool, StoreError> {
        let start_ns = event.timerange.lower_nanos();
        let end_ns = event.timerange.upper_nanos();
        let event_id = event.id.to_string();
        let sk = event_sort_key(start_ns, &event_id);

        // Serialize the full Event once; this is also the item-size guard. TADS stores small
        // inline metadata, so a payload that would push the item past the limit is rejected as a
        // client error rather than offloaded (large data belongs in TAMS).
        let doc =
            serde_json::to_string(&event).map_err(|e| StoreError::Serialization(e.to_string()))?;
        if doc.len() > self.max_payload_bytes {
            return Err(StoreError::PayloadTooLarge {
                size: doc.len(),
                limit: self.max_payload_bytes,
            });
        }

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
        item.insert("doc".to_string(), AttributeValue::S(doc));

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

    /// Delete every Event wholly contained within `timerange` (an eternity range deletes all
    /// Events on the track — the spec deletes Events that "wholly overlap" the window).
    /// Paginates internally and removes items in BatchWriteItem chunks of 25, retrying any
    /// unprocessed keys. Returns the number of Events deleted.
    pub async fn delete_all_within(
        &self,
        track_id: &str,
        timerange: &TimeRange,
    ) -> Result<usize, StoreError> {
        let mut deleted = 0usize;
        let mut page = PageParams {
            limit: Some(1000),
            page: None,
        };
        loop {
            let query = EventQuery {
                timerange: *timerange,
                ids: Vec::new(),
                reverse_order: false,
            };
            let result = self.query(track_id, &query, &page).await?;

            let keys: Vec<String> = result
                .items
                .iter()
                .filter(|e| timerange.contains_range(&e.timerange))
                .map(|e| event_sort_key(e.timerange.lower_nanos(), &e.id.to_string()))
                .collect();

            for chunk in keys.chunks(25) {
                self.batch_delete(track_id, chunk).await?;
                deleted += chunk.len();
            }

            match result.next_key {
                Some(k) => {
                    page = PageParams {
                        limit: Some(1000),
                        page: Some(k),
                    }
                }
                None => break,
            }
        }
        Ok(deleted)
    }

    /// Delete up to 25 Events by sort key in one BatchWriteItem, retrying unprocessed keys.
    async fn batch_delete(&self, track_id: &str, sort_keys: &[String]) -> Result<(), StoreError> {
        use aws_sdk_dynamodb::types::{DeleteRequest, WriteRequest};

        let mut requests: Vec<WriteRequest> = sort_keys
            .iter()
            .map(|sort_key| {
                let key = HashMap::from([
                    (
                        "track_id".to_string(),
                        AttributeValue::S(track_id.to_string()),
                    ),
                    ("sk".to_string(), AttributeValue::S(sort_key.clone())),
                ]);
                let delete = DeleteRequest::builder()
                    .set_key(Some(key))
                    .build()
                    .map_err(StoreError::dynamo)?;
                Ok(WriteRequest::builder().delete_request(delete).build())
            })
            .collect::<Result<_, StoreError>>()?;

        for _ in 0..5 {
            if requests.is_empty() {
                break;
            }
            let out = self
                .dynamo
                .batch_write_item()
                .request_items(self.table.clone(), std::mem::take(&mut requests))
                .send()
                .await
                .map_err(StoreError::dynamo)?;
            if let Some(unprocessed) = out
                .unprocessed_items
                .and_then(|mut m| m.remove(&self.table))
            {
                requests = unprocessed;
            }
        }

        if !requests.is_empty() {
            return Err(StoreError::Dynamo(
                "batch delete left unprocessed items after retries".into(),
            ));
        }
        Ok(())
    }

    fn parse_event(item: &HashMap<String, AttributeValue>) -> Result<TrackEvent, StoreError> {
        let doc = item
            .get("doc")
            .and_then(|v| v.as_s().ok())
            .ok_or_else(|| StoreError::Malformed("event item missing `doc`".into()))?;
        serde_json::from_str(doc).map_err(|e| StoreError::Malformed(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput;
    use aws_sdk_dynamodb::operation::query::QueryOutput;
    use aws_smithy_mocks::{RuleMode, mock, mock_client};

    fn event_item(id: &str, timerange: &str) -> HashMap<String, AttributeValue> {
        let doc =
            serde_json::json!({ "id": id, "timerange": timerange, "payload": {} }).to_string();
        HashMap::from([("doc".to_string(), AttributeValue::S(doc))])
    }

    #[tokio::test]
    async fn delete_all_within_removes_only_wholly_contained_events() {
        // One Event wholly inside [0:0_100:0), one only partially overlapping it.
        let contained = event_item("2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f", "[10:0_20:0)");
        let partial = event_item("3e2f3c4d-5b6c-4d7e-9faf-1b2c3d4e5f60", "[50:0_150:0)");

        let query_rule = mock!(aws_sdk_dynamodb::Client::query).then_output(move || {
            QueryOutput::builder()
                .items(contained.clone())
                .items(partial.clone())
                .build()
        });
        // Capture how many keys the single batch delete carried.
        let batch_rule = mock!(aws_sdk_dynamodb::Client::batch_write_item)
            .match_requests(|req| {
                req.request_items()
                    .and_then(|m| m.get("tads-events"))
                    .map(|w| w.len() == 1)
                    .unwrap_or(false)
            })
            .then_output(|| BatchWriteItemOutput::builder().build());

        let dynamo = mock_client!(
            aws_sdk_dynamodb,
            RuleMode::MatchAny,
            [&query_rule, &batch_rule]
        );
        let repo = EventRepo::new(dynamo, "tads-events", 360 * 1024);

        let deleted = repo
            .delete_all_within("track-1", &"[0:0_100:0)".parse::<TimeRange>().unwrap())
            .await
            .unwrap();

        assert_eq!(deleted, 1, "only the wholly-contained Event is deleted");
        assert_eq!(
            batch_rule.num_calls(),
            1,
            "exactly one batch-delete with one key"
        );
    }
}
