//! Search-indexer library (DynamoDB-Streams-triggered).
//!
//! Projects TADS metadata into OpenSearch so clients can "search by the data itself". Pure
//! Streams-record → index-operation mapping lives in [`derive_op`] (unit-tested); the actual
//! writes go through the [`IndexSink`] trait, implemented by [`OpenSearchSink`]. The indexer is
//! strictly downstream of the system-of-record and can be rebuilt from a table/Streams replay.

use serde_json::{Value, json};

/// One index mutation derived from a change record.
#[derive(Clone, Debug, PartialEq)]
pub enum IndexOp {
    /// Create or replace a document (INSERT/MODIFY).
    Upsert {
        index: String,
        id: String,
        body: Value,
    },
    /// Remove a document (REMOVE).
    Delete { index: String, id: String },
}

/// OpenSearch index names per TADS resource (map 1:1 to the DynamoDB table names).
#[derive(Clone, Debug)]
pub struct Indices {
    pub schemas: String,
    pub tracks: String,
    pub events: String,
}

#[derive(Clone, Copy)]
enum Resource {
    Schema,
    Track,
    Event,
}

impl Indices {
    fn resolve(&self, table: &str) -> Option<(Resource, &str)> {
        if table == self.schemas {
            Some((Resource::Schema, &self.schemas))
        } else if table == self.tracks {
            Some((Resource::Track, &self.tracks))
        } else if table == self.events {
            Some((Resource::Event, &self.events))
        } else {
            None
        }
    }
}

fn table_from_arn(arn: &str) -> Option<&str> {
    arn.split("table/").nth(1).and_then(|s| s.split('/').next())
}

fn image_doc(image: &Value) -> Option<Value> {
    let s = image.get("doc")?.get("S")?.as_str()?;
    serde_json::from_str(s).ok()
}

fn attr_s(image: &Value, name: &str) -> Option<String> {
    image.get(name)?.get("S")?.as_str().map(str::to_string)
}

fn attr_n(image: &Value, name: &str) -> Option<i64> {
    image.get(name)?.get("N")?.as_str()?.parse().ok()
}

/// Map a DynamoDB Streams record to an index operation, or `None` if it is not an indexable
/// change. Event documents are enriched with `track_id`/`start_ns`/`end_ns` for range search.
pub fn derive_op(record: &Value, indices: &Indices) -> Option<IndexOp> {
    let event_name = record.get("eventName")?.as_str()?;
    let table = table_from_arn(record.get("eventSourceARN")?.as_str()?)?;
    let (resource, index) = indices.resolve(table)?;
    let index = index.to_string();
    let ddb = record.get("dynamodb")?;
    let new_image = ddb.get("NewImage");
    let keys = ddb.get("Keys");
    let old_image = ddb.get("OldImage");

    let id_attr = match resource {
        Resource::Schema => "schema_id",
        Resource::Track => "track_id",
        Resource::Event => "event_id",
    };

    if event_name == "REMOVE" {
        let source = old_image.or(keys)?;
        let id = doc_id(source, resource).or_else(|| attr_s(source, id_attr))?;
        return Some(IndexOp::Delete { index, id });
    }

    // INSERT / MODIFY → upsert the document.
    let image = new_image?;
    let mut body = image_doc(image)?;
    let id = doc_id_from_doc(&body, resource).or_else(|| attr_s(image, id_attr))?;

    if matches!(resource, Resource::Event) {
        // Enrich event docs with the projected search/range attributes.
        if let Some(obj) = body.as_object_mut() {
            if let Some(tid) = attr_s(image, "track_id") {
                obj.insert("track_id".to_string(), json!(tid));
            }
            if let Some(start) = attr_n(image, "start_ns") {
                obj.insert("start_ns".to_string(), json!(start));
            }
            if let Some(end) = attr_n(image, "end_ns") {
                obj.insert("end_ns".to_string(), json!(end));
            }
        }
    }

    Some(IndexOp::Upsert { index, id, body })
}

fn doc_id_from_doc(doc: &Value, resource: Resource) -> Option<String> {
    let key = match resource {
        Resource::Schema | Resource::Track | Resource::Event => "id",
    };
    doc.get(key).and_then(Value::as_str).map(str::to_string)
}

fn doc_id(image: &Value, resource: Resource) -> Option<String> {
    image_doc(image).and_then(|d| doc_id_from_doc(&d, resource))
}

/// Where index operations are applied. Abstracted so the indexing logic is testable without a
/// live cluster.
pub trait IndexSink {
    fn apply(&self, op: IndexOp) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}

/// Applies derived operations to a sink.
pub struct Indexer<S: IndexSink> {
    sink: S,
    indices: Indices,
}

impl<S: IndexSink> Indexer<S> {
    pub fn new(sink: S, indices: Indices) -> Self {
        Self { sink, indices }
    }

    pub async fn index_batch(&self, records: &[Value]) -> anyhow::Result<()> {
        for record in records {
            if let Some(op) = derive_op(record, &self.indices) {
                self.sink.apply(op).await?;
            }
        }
        Ok(())
    }
}

/// [`IndexSink`] backed by an OpenSearch (Serverless) cluster, signing requests with SigV4 for
/// the `aoss` service. Index-per-resource; document id is the resource id.
pub struct OpenSearchSink {
    client: opensearch::OpenSearch,
}

impl OpenSearchSink {
    /// Build a client for `endpoint` using the ambient AWS credentials/region.
    pub fn new(endpoint: &str, sdk: &aws_types::SdkConfig) -> anyhow::Result<Self> {
        use opensearch::auth::Credentials;
        use opensearch::http::Url;
        use opensearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};

        let pool = SingleNodeConnectionPool::new(Url::parse(endpoint)?);
        let credentials = Credentials::AwsSigV4(
            sdk.credentials_provider()
                .ok_or_else(|| anyhow::anyhow!("no AWS credentials available"))?,
            sdk.region()
                .ok_or_else(|| anyhow::anyhow!("no AWS region configured"))?
                .clone(),
        );
        let transport = TransportBuilder::new(pool)
            .auth(credentials)
            .service_name("aoss")
            .build()?;
        Ok(Self {
            client: opensearch::OpenSearch::new(transport),
        })
    }
}

impl IndexSink for OpenSearchSink {
    async fn apply(&self, op: IndexOp) -> anyhow::Result<()> {
        match op {
            IndexOp::Upsert { index, id, body } => {
                let resp = self
                    .client
                    .index(opensearch::IndexParts::IndexId(&index, &id))
                    .body(body)
                    .send()
                    .await?;
                let status = resp.status_code();
                if !status.is_success() {
                    anyhow::bail!("opensearch index {index}/{id} returned {status}");
                }
            }
            IndexOp::Delete { index, id } => {
                let resp = self
                    .client
                    .delete(opensearch::DeleteParts::IndexId(&index, &id))
                    .send()
                    .await?;
                let status = resp.status_code();
                // A delete of an already-absent document (404) is fine.
                if !status.is_success() && status.as_u16() != 404 {
                    anyhow::bail!("opensearch delete {index}/{id} returned {status}");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn indices() -> Indices {
        Indices {
            schemas: "tads-schemas".into(),
            tracks: "tads-tracks".into(),
            events: "tads-events".into(),
        }
    }

    #[test]
    fn derives_track_upsert() {
        let track = json!({ "id": "track-1", "schema_id": "s1" });
        let record = json!({
            "eventName": "MODIFY",
            "eventSourceARN": "arn:aws:dynamodb:eu-west-1:1:table/tads-tracks/stream/x",
            "dynamodb": { "NewImage": { "doc": { "S": track.to_string() } } }
        });
        match derive_op(&record, &indices()).unwrap() {
            IndexOp::Upsert { index, id, body } => {
                assert_eq!(index, "tads-tracks");
                assert_eq!(id, "track-1");
                assert_eq!(body["schema_id"], "s1");
            }
            other => panic!("expected upsert, got {other:?}"),
        }
    }

    #[test]
    fn derives_event_upsert_with_range_attrs() {
        let ev = json!({ "id": "e1", "timerange": "[0:0_1:0)", "payload": { "text": "hi" } });
        let record = json!({
            "eventName": "INSERT",
            "eventSourceARN": "arn:aws:dynamodb:eu-west-1:1:table/tads-events/stream/x",
            "dynamodb": { "NewImage": {
                "doc": { "S": ev.to_string() },
                "track_id": { "S": "track-1" },
                "start_ns": { "N": "0" },
                "end_ns": { "N": "1000000000" }
            } }
        });
        match derive_op(&record, &indices()).unwrap() {
            IndexOp::Upsert { index, id, body } => {
                assert_eq!(index, "tads-events");
                assert_eq!(id, "e1");
                assert_eq!(body["track_id"], "track-1");
                assert_eq!(body["start_ns"], 0);
                assert_eq!(body["end_ns"], 1_000_000_000i64);
            }
            other => panic!("expected upsert, got {other:?}"),
        }
    }

    #[test]
    fn derives_delete_on_remove() {
        let old = json!({ "id": "s1" });
        let record = json!({
            "eventName": "REMOVE",
            "eventSourceARN": "arn:aws:dynamodb:eu-west-1:1:table/tads-schemas/stream/x",
            "dynamodb": { "OldImage": { "doc": { "S": old.to_string() } } }
        });
        assert_eq!(
            derive_op(&record, &indices()).unwrap(),
            IndexOp::Delete {
                index: "tads-schemas".into(),
                id: "s1".into()
            }
        );
    }

    #[derive(Default)]
    struct MockSink {
        ops: Mutex<Vec<IndexOp>>,
    }
    impl IndexSink for MockSink {
        async fn apply(&self, op: IndexOp) -> anyhow::Result<()> {
            self.ops.lock().unwrap().push(op);
            Ok(())
        }
    }

    #[tokio::test]
    async fn indexer_applies_each_derivable_record() {
        let track = json!({ "id": "track-1" });
        let records = vec![
            json!({
                "eventName": "INSERT",
                "eventSourceARN": "arn:aws:dynamodb:eu-west-1:1:table/tads-tracks/stream/x",
                "dynamodb": { "NewImage": { "doc": { "S": track.to_string() } } }
            }),
            json!({
                "eventName": "INSERT",
                "eventSourceARN": "arn:aws:dynamodb:eu-west-1:1:table/other/stream/x",
                "dynamodb": { "NewImage": {} }
            }),
        ];
        let indexer = Indexer::new(MockSink::default(), indices());
        indexer.index_batch(&records).await.unwrap();
        assert_eq!(
            indexer.sink.ops.lock().unwrap().len(),
            1,
            "only the known-table record indexed"
        );
    }
}
