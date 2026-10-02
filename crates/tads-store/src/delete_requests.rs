//! Track Delete Request persistence (`tads-delete-requests` table) + SQS enqueue.
//!
//! A `DELETE /tracks/{id}` (or bulk event delete) creates a request here (status `created`),
//! enqueues an SQS message, and returns 202 + `Location`. The Phase 2 delete-worker consumes the
//! queue and advances the status. `expiry` is a DynamoDB TTL attribute.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_sqs::Client as SqsClient;
use tads_domain::TrackDeleteRequest;

use crate::cursor::{Page, PageParams, encode_cursor};
use crate::error::StoreError;

/// Sort field for `GET /track-delete-requests`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteSortBy {
    Created,
    Expiry,
}

#[derive(Debug, Clone)]
pub struct DeleteRequestRepo {
    dynamo: DynamoClient,
    sqs: SqsClient,
    table: String,
    queue_url: String,
}

impl DeleteRequestRepo {
    pub fn new(
        dynamo: DynamoClient,
        sqs: SqsClient,
        table: impl Into<String>,
        queue_url: impl Into<String>,
    ) -> Self {
        Self {
            dynamo,
            sqs,
            table: table.into(),
            queue_url: queue_url.into(),
        }
    }

    /// Persist a new delete request and enqueue it for the worker.
    pub async fn create(&self, request: &TrackDeleteRequest) -> Result<(), StoreError> {
        let doc =
            serde_json::to_string(request).map_err(|e| StoreError::Serialization(e.to_string()))?;

        let mut item = HashMap::new();
        item.insert(
            "request_id".to_string(),
            AttributeValue::S(request.id.to_string()),
        );
        item.insert("doc".to_string(), AttributeValue::S(doc));
        if let Some(expiry) = &request.expiry {
            // DynamoDB TTL expects epoch seconds.
            item.insert(
                "expiry".to_string(),
                AttributeValue::N(expiry.timestamp().to_string()),
            );
        }

        self.dynamo
            .put_item()
            .table_name(&self.table)
            .set_item(Some(item))
            .send()
            .await
            .map_err(StoreError::dynamo)?;

        self.sqs
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(request.id.to_string())
            .send()
            .await
            .map_err(StoreError::dynamo)?;
        Ok(())
    }

    pub async fn get(&self, id: &str) -> Result<Option<TrackDeleteRequest>, StoreError> {
        let out = self
            .dynamo
            .get_item()
            .table_name(&self.table)
            .key("request_id", AttributeValue::S(id.to_string()))
            .send()
            .await
            .map_err(StoreError::dynamo)?;
        out.item.map(|item| parse_doc(&item)).transpose()
    }

    pub async fn list(
        &self,
        _sort_by: DeleteSortBy,
        _reverse_order: bool,
        page: &PageParams,
    ) -> Result<Page<TrackDeleteRequest>, StoreError> {
        // Phase 1: scan with cursor pagination. The spec permits an incomplete/unordered listing;
        // Phase 2 adds the sort GSI for `sort_by`/`reverse_order`.
        let limit = page.effective_limit();
        let start = page.exclusive_start_key()?;
        let out = self
            .dynamo
            .scan()
            .table_name(&self.table)
            .limit(limit)
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(StoreError::dynamo)?;

        let items: Vec<TrackDeleteRequest> = out
            .items
            .unwrap_or_default()
            .iter()
            .map(parse_doc)
            .collect::<Result<_, _>>()?;
        let next_key = out
            .last_evaluated_key
            .as_ref()
            .map(encode_cursor)
            .transpose()?;
        Ok(Page {
            items,
            next_key,
            limit,
        })
    }
}

fn parse_doc(item: &HashMap<String, AttributeValue>) -> Result<TrackDeleteRequest, StoreError> {
    let doc = item
        .get("doc")
        .and_then(|v| v.as_s().ok())
        .ok_or_else(|| StoreError::Malformed("delete-request item missing `doc`".into()))?;
    serde_json::from_str(doc).map_err(|e| StoreError::Malformed(e.to_string()))
}
