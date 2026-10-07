//! Schema-migration Lambda, invoked as a CloudFormation custom resource (via `cr.Provider`) on
//! stack create/update. Connects to Aurora through RDS Proxy and runs the embedded sqlx
//! migrations (including `CREATE EXTENSION btree_gist`). Delete is a no-op.

use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tads_telemetry::init();

    lambda_runtime::run(service_fn(|event: LambdaEvent<Value>| async move {
        let request_type = event
            .payload
            .get("RequestType")
            .and_then(Value::as_str)
            .unwrap_or("Create");
        let physical_id = json!({ "PhysicalResourceId": "tads-migrate" });

        if request_type == "Delete" {
            return Ok::<Value, Error>(physical_id);
        }

        let (pool, _config, _aws) = tads_http::connect_pool()
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        tads_store::migrate(&pool)
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        tracing::info!("migrations applied");
        Ok::<Value, Error>(physical_id)
    }))
    .await
}
