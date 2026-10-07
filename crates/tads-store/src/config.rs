/// Settings the store needs, resolved from application config/env (see `tads-http`).
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Postgres connection settings (Aurora via RDS Proxy).
    pub database: DatabaseConfig,
    /// SQS queue the tracks/events handlers enqueue delete-request work onto.
    pub delete_queue_url: String,
    /// Maximum serialized Event size (bytes). A larger Event is rejected (413) rather than
    /// stored — TADS holds small inline metadata; large data belongs in TAMS. Postgres has no
    /// hard row limit, but this stays an API contract carried over from the DynamoDB design.
    pub max_payload_bytes: usize,
}

/// How to reach Aurora. When `password` is empty the pool builder mints an RDS IAM auth token
/// against `proxy_endpoint`/`region`; otherwise the supplied password (e.g. from Secrets Manager)
/// is used directly.
#[derive(Debug, Clone, Default)]
pub struct DatabaseConfig {
    pub proxy_endpoint: String,
    pub port: u16,
    pub db_name: String,
    pub username: String,
    pub region: String,
    /// Optional static password; empty => use IAM auth-token generation.
    pub password: String,
    pub max_connections: u32,
}

impl StoreConfig {
    /// Default max Event size (360 KB) — unchanged from the DynamoDB design so the 413 behaviour
    /// is identical across backends.
    pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 360 * 1024;
}
