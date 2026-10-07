//! Webhook-dispatcher library (transactional-outbox-driven).
//!
//! Maps an outbox row (written in the same transaction as a resource mutation) to a TADS webhook
//! event, matches it against each registered subscription's filters, and POSTs the callback body
//! to the subscriber `url` with the `api_key_name` header. Pure mapping/matching lives in free
//! functions (unit-tested); delivery + status updates are on [`Dispatcher`].

use chrono::Utc;
use reqwest::Client as HttpClient;
use serde_json::{Value, json};
use tads_domain::tags::TagValue;
use tads_domain::{Tags, WebhookEvent, WebhookGet, WebhookStatus};
use tads_store::{OutboxRow, StoredWebhook, WebhookRepo};

/// A derived webhook event plus the fields needed to apply subscription filters.
#[derive(Clone, Debug)]
pub struct Change {
    pub event_type: WebhookEvent,
    pub track_id: Option<String>,
    pub schema_id: Option<String>,
    pub entity_id: Option<String>,
    pub editable_events: Option<bool>,
    pub tags: Option<Tags>,
    /// The callback `event` object (type-specific contents).
    pub event: Value,
}

fn parse_tags(doc: &Value) -> Option<Tags> {
    doc.get("tags")
        .and_then(|t| serde_json::from_value(t.clone()).ok())
}

fn key_str(key_fields: &Value, name: &str) -> Option<String> {
    key_fields
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Derive the webhook event from an outbox row, or `None` if it is not a change we publish.
/// `row.doc` is the new document (for insert/modify); `row.key_fields` carries the ids needed for
/// removes. `row.aggregate_id` is the track id for events, else the resource id.
pub fn from_outbox(row: &OutboxRow) -> Option<Change> {
    use WebhookEvent::*;
    let created = row.op == tads_store::outbox::OP_INSERT;

    match row.resource.as_str() {
        "schema" => match row.op.as_str() {
            "insert" | "modify" => {
                let doc = row.doc.clone()?;
                Some(Change {
                    event_type: if created {
                        SchemasCreated
                    } else {
                        SchemasUpdated
                    },
                    track_id: None,
                    schema_id: doc.get("id").and_then(Value::as_str).map(str::to_string),
                    entity_id: None,
                    editable_events: None,
                    tags: parse_tags(&doc),
                    event: json!({ "schema": doc }),
                })
            }
            "remove" => {
                let id = key_str(&row.key_fields, "schema_id")?;
                Some(Change {
                    event_type: SchemasDeleted,
                    track_id: None,
                    schema_id: Some(id.clone()),
                    entity_id: None,
                    editable_events: None,
                    tags: None,
                    event: json!({ "schema_id": id }),
                })
            }
            _ => None,
        },
        "track" => match row.op.as_str() {
            "insert" | "modify" => {
                let doc = row.doc.clone()?;
                Some(Change {
                    event_type: if created {
                        TracksCreated
                    } else {
                        TracksUpdated
                    },
                    track_id: doc.get("id").and_then(Value::as_str).map(str::to_string),
                    schema_id: doc
                        .get("schema_id")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    entity_id: doc
                        .get("timeline_reference")
                        .and_then(|t| t.get("entity_id"))
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    editable_events: doc.get("editable_events").and_then(Value::as_bool),
                    tags: parse_tags(&doc),
                    event: json!({ "track": doc }),
                })
            }
            "remove" => {
                let id = key_str(&row.key_fields, "track_id")?;
                Some(Change {
                    event_type: TracksDeleted,
                    track_id: Some(id.clone()),
                    schema_id: None,
                    entity_id: None,
                    editable_events: None,
                    tags: None,
                    event: json!({ "track_id": id }),
                })
            }
            _ => None,
        },
        "event" => {
            let track_id =
                key_str(&row.key_fields, "track_id").or_else(|| Some(row.aggregate_id.clone()));
            match row.op.as_str() {
                "insert" | "modify" => {
                    let doc = row.doc.clone()?;
                    Some(Change {
                        event_type: if created {
                            TracksEventsAdded
                        } else {
                            TracksEventsUpdated
                        },
                        track_id: track_id.clone(),
                        schema_id: None,
                        entity_id: None,
                        editable_events: None,
                        tags: None,
                        event: json!({ "track_id": track_id, "track_events": [doc] }),
                    })
                }
                "remove" => {
                    let event_id = key_str(&row.key_fields, "event_id");
                    Some(Change {
                        event_type: TracksEventsDeleted,
                        track_id: track_id.clone(),
                        schema_id: None,
                        entity_id: None,
                        editable_events: None,
                        tags: None,
                        event: json!({ "track_id": track_id, "track_event_ids": event_id.into_iter().collect::<Vec<_>>() }),
                    })
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Does `webhook` subscribe to this change (event type + filters)? Filters are only applied when
/// the corresponding fact is known for the change.
pub fn webhook_matches(webhook: &WebhookGet, change: &Change) -> bool {
    if !matches!(
        webhook.status,
        WebhookStatus::Created | WebhookStatus::Started
    ) {
        return false;
    }
    if !webhook.webhook.events.contains(&change.event_type) {
        return false;
    }
    if let (Some(filter), Some(tid)) = (&webhook.webhook.track_ids, &change.track_id)
        && !filter.iter().any(|f| &f.to_string() == tid)
    {
        return false;
    }
    if let (Some(filter), Some(sid)) = (&webhook.webhook.schema_ids, &change.schema_id)
        && !filter.iter().any(|f| &f.to_string() == sid)
    {
        return false;
    }
    if let (Some(filter), Some(ce)) = (webhook.webhook.editable_events, change.editable_events)
        && filter != ce
    {
        return false;
    }
    if let (Some(filter), Some(eid)) = (
        &webhook.webhook.timeline_reference_entity_id,
        &change.entity_id,
    ) && !filter.contains(eid)
    {
        return false;
    }
    if !webhook.webhook.tags.is_empty()
        && let Some(obj_tags) = &change.tags
    {
        for (k, v) in &webhook.webhook.tags.0 {
            let ok = match v {
                TagValue::One(x) => obj_tags.matches(k, x),
                TagValue::Many(xs) => xs.iter().all(|x| obj_tags.matches(k, x)),
            };
            if !ok {
                return false;
            }
        }
    }
    true
}

/// Build the full callback body (`event_timestamp`, `event_type`, `event`).
pub fn callback_body(change: &Change) -> Value {
    let type_str = serde_json::to_value(change.event_type)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default();
    json!({
        "event_timestamp": Utc::now().to_rfc3339(),
        "event_type": type_str,
        "event": change.event,
    })
}

/// Delivers matched events to subscribers.
#[derive(Clone)]
pub struct Dispatcher {
    repo: WebhookRepo,
    http: HttpClient,
    max_attempts: u32,
}

impl Dispatcher {
    pub fn new(repo: WebhookRepo, http: HttpClient) -> Self {
        Self {
            repo,
            http,
            max_attempts: 3,
        }
    }

    /// Process a batch of outbox rows: load subscriptions once, then fan out matches.
    pub async fn dispatch(&self, rows: &[OutboxRow]) -> anyhow::Result<()> {
        let webhooks = self.repo.list_all().await?;
        if webhooks.is_empty() {
            return Ok(());
        }
        for row in rows {
            let Some(change) = from_outbox(row) else {
                continue;
            };
            let body = callback_body(&change);
            for stored in &webhooks {
                if webhook_matches(&stored.webhook, &change) {
                    self.deliver(stored, &body).await;
                }
            }
        }
        Ok(())
    }

    /// POST to one subscriber, retrying transient failures; drive the webhook to `error` if all
    /// attempts fail.
    async fn deliver(&self, stored: &StoredWebhook, body: &Value) {
        let url = &stored.webhook.webhook.url;
        let mut last_err = String::new();

        for attempt in 1..=self.max_attempts {
            let mut req = self.http.post(url).json(body);
            if let (Some(name), Some(value)) =
                (&stored.webhook.webhook.api_key_name, &stored.api_key_value)
            {
                req = req.header(name, value);
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    tracing::info!(webhook_id = %stored.webhook.id, url, "delivered");
                    return;
                }
                Ok(resp) => last_err = format!("subscriber returned {}", resp.status()),
                Err(e) => last_err = e.to_string(),
            }
            tracing::warn!(webhook_id = %stored.webhook.id, attempt, error = %last_err, "delivery attempt failed");
        }

        tracing::error!(webhook_id = %stored.webhook.id, url, error = %last_err, "delivery failed; marking webhook error");
        let mut errored = stored.clone();
        errored.webhook.status = WebhookStatus::Error;
        errored.webhook.error = Some(tads_domain::ErrorBody {
            kind: "delivery_failed".to_string(),
            summary: last_err,
            traceback: None,
            time: Utc::now(),
        });
        if let Err(e) = self.repo.put_stored(&errored).await {
            tracing::error!(webhook_id = %stored.webhook.id, error = %e, "failed to persist error status");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn track_insert_row() -> OutboxRow {
        let track = json!({
            "id": "2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f",
            "schema_id": "3e2f3c4d-5b6c-4d7e-9faf-1b2c3d4e5f60",
            "timeline_reference": { "entity_id": "tams://flow/abc" },
            "editable_events": true,
            "tags": { "team": "news" }
        });
        OutboxRow {
            id: 1,
            op: "insert".into(),
            resource: "track".into(),
            aggregate_id: "2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f".into(),
            doc: Some(track),
            key_fields: json!({ "track_id": "2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f" }),
        }
    }

    #[test]
    fn derives_track_created_with_filter_fields() {
        let change = from_outbox(&track_insert_row()).unwrap();
        assert_eq!(change.event_type, WebhookEvent::TracksCreated);
        assert_eq!(change.entity_id.as_deref(), Some("tams://flow/abc"));
        assert_eq!(change.editable_events, Some(true));
        assert!(change.event.get("track").is_some());
    }

    #[test]
    fn derives_event_added() {
        let ev = json!({ "id": "2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f", "timerange": "[0:0_1:0)", "payload": {} });
        let row = OutboxRow {
            id: 2,
            op: "insert".into(),
            resource: "event".into(),
            aggregate_id: "track-1".into(),
            doc: Some(ev),
            key_fields: json!({ "track_id": "track-1", "event_id": "2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f" }),
        };
        let change = from_outbox(&row).unwrap();
        assert_eq!(change.event_type, WebhookEvent::TracksEventsAdded);
        assert_eq!(change.track_id.as_deref(), Some("track-1"));
        assert_eq!(change.event["track_events"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn unknown_resource_is_ignored() {
        let row = OutboxRow {
            id: 3,
            op: "insert".into(),
            resource: "other".into(),
            aggregate_id: "x".into(),
            doc: Some(json!({})),
            key_fields: json!({}),
        };
        assert!(from_outbox(&row).is_none());
    }

    #[test]
    fn matches_requires_subscribed_event_and_active_status() {
        use tads_domain::{TadsId, Webhook};
        let change = from_outbox(&track_insert_row()).unwrap();

        let base = Webhook {
            url: "https://x".into(),
            api_key_name: None,
            events: vec![WebhookEvent::TracksCreated],
            schema_ids: None,
            track_ids: None,
            timeline_reference_entity_id: None,
            editable_events: None,
            tags: Tags::default(),
        };
        let active = WebhookGet {
            id: TadsId::new_v4(),
            webhook: base.clone(),
            status: WebhookStatus::Created,
            error: None,
        };
        assert!(webhook_matches(&active, &change));

        let mut other = active.clone();
        other.webhook.events = vec![WebhookEvent::SchemasCreated];
        assert!(!webhook_matches(&other, &change));

        let mut disabled = active.clone();
        disabled.status = WebhookStatus::Disabled;
        assert!(!webhook_matches(&disabled, &change));

        let mut filtered = active.clone();
        filtered.webhook.editable_events = Some(false);
        assert!(!webhook_matches(&filtered, &change));
    }
}
