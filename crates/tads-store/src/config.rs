/// Resource names the store needs, resolved from application config/env (see `tads-http`).
#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub schemas_table: String,
    pub tracks_table: String,
    pub events_table: String,
    pub webhooks_table: String,
    pub delete_requests_table: String,
    /// Bucket for Event-payload overflow (payloads approaching the DynamoDB item limit).
    pub overflow_bucket: String,
    /// SQS queue the tracks/events handlers enqueue delete-request work onto.
    pub delete_queue_url: String,
    /// Serialized-payload size (bytes) above which an Event payload is offloaded to S3.
    /// DynamoDB's hard item limit is 400 KB; we leave headroom for the surrounding attributes.
    pub overflow_threshold_bytes: usize,
}

impl StoreConfig {
    /// Sensible overflow threshold: 360 KB leaves room for keys, timerange, and metadata inside
    /// the 400 KB DynamoDB item ceiling.
    pub const DEFAULT_OVERFLOW_THRESHOLD: usize = 360 * 1024;
}
