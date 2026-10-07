//! Postgres connection pool construction (Aurora via RDS Proxy).
//!
//! The store is AWS-agnostic: it receives a resolved [`DatabaseConfig`] (host/port/db/user +
//! password already fetched from Secrets Manager by `tads-http`) and builds a TLS pool against
//! the RDS Proxy endpoint. `require` SSL mode matches the proxy's `require_tls=true`.

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};

use crate::config::DatabaseConfig;
use crate::error::StoreError;

/// Build a connection pool against the RDS Proxy endpoint.
pub async fn connect(cfg: &DatabaseConfig) -> Result<PgPool, StoreError> {
    let opts = PgConnectOptions::new()
        .host(&cfg.proxy_endpoint)
        .port(cfg.port)
        .database(&cfg.db_name)
        .username(&cfg.username)
        .password(&cfg.password)
        .ssl_mode(PgSslMode::Require);

    PgPoolOptions::new()
        .max_connections(cfg.max_connections.max(1))
        .connect_with(opts)
        .await
        .map_err(StoreError::from)
}

/// Run embedded migrations (used by the `migrate` Lambda and tests).
pub async fn migrate(pool: &PgPool) -> Result<(), StoreError> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))
}
