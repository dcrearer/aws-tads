//! Shared application state (repositories + service metadata) injected into every handler.

use std::sync::Arc;

use aws_config::BehaviorVersion;
use tads_domain::Service;
use tads_store::{DeleteRequestRepo, EventRepo, SchemaRepo, StoreConfig, TrackRepo};

use crate::config::AppConfig;
use crate::error::AppError;

/// Cloneable handle to the store repositories and static service metadata. Repos wrap AWS SDK
/// clients (cheap to clone), so `AppState` is `Clone` and shared across requests.
#[derive(Clone)]
pub struct AppState {
    pub schemas: SchemaRepo,
    pub tracks: TrackRepo,
    pub events: EventRepo,
    pub delete_requests: DeleteRequestRepo,
    pub service: Arc<Service>,
}

impl AppState {
    /// Build state from configuration and the ambient AWS environment (the Lambda execution
    /// role / region). Called once per cold start.
    pub async fn from_env() -> Result<Self, AppError> {
        let config = AppConfig::load()?;
        let store = config.store_config();

        let aws = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let dynamo = aws_sdk_dynamodb::Client::new(&aws);
        let sqs = aws_sdk_sqs::Client::new(&aws);

        Ok(Self::from_parts(config, store, dynamo, sqs))
    }

    /// Assemble state from explicit clients — the seam tests use with `aws-smithy-mocks`.
    pub fn from_parts(
        config: AppConfig,
        store: StoreConfig,
        dynamo: aws_sdk_dynamodb::Client,
        sqs: aws_sdk_sqs::Client,
    ) -> Self {
        let service = Service {
            name: config.service.name.clone(),
            description: None,
            service_type: config.service.service_type.clone(),
            api_version: config.service.api_version.clone(),
            service_version: config.service.service_version.clone(),
        };

        Self {
            schemas: SchemaRepo::new(dynamo.clone(), store.schemas_table.clone()),
            tracks: TrackRepo::new(dynamo.clone(), store.tracks_table.clone()),
            events: EventRepo::new(
                dynamo.clone(),
                store.events_table.clone(),
                store.max_payload_bytes,
            ),
            delete_requests: DeleteRequestRepo::new(
                dynamo,
                sqs,
                store.delete_requests_table.clone(),
                store.delete_queue_url.clone(),
            ),
            service: Arc::new(service),
        }
    }
}
