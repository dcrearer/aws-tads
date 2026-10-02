/// Resource names the store needs, resolved from application config/env (see `tads-http`).
#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub schemas_table: String,
    pub tracks_table: String,
    pub events_table: String,
    pub webhooks_table: String,
    pub delete_requests_table: String,
    /// SQS queue the tracks/events handlers enqueue delete-request work onto.
    pub delete_queue_url: String,
    /// Maximum serialized Event size (bytes). A larger Event is rejected (413) rather than
    /// stored — TADS holds small inline metadata; large data belongs in TAMS. Kept under
    /// DynamoDB's 400 KB item limit, leaving headroom for the surrounding attributes.
    pub max_payload_bytes: usize,
}

impl StoreConfig {
    /// Default max Event size: 360 KB leaves room for keys, timerange, and metadata inside the
    /// 400 KB DynamoDB item ceiling.
    pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 360 * 1024;
}
