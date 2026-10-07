"""TADS API infrastructure stack.

Provisions, per the project design:
- 5 DynamoDB tables (payloads inline), all with Streams (NEW_AND_OLD_IMAGES)
- An SQS queue (+ DLQ) for async delete requests
- 9 Lambda functions (Rust `provided.al2023`, arm64): 6 HTTP + 3 Streams/SQS workers
- An HTTP API Gateway fronted by a Cognito JWT authorizer
- An OpenSearch Serverless collection (search-indexer target)
- Least-privilege IAM grants + event-source mappings + X-Ray active tracing

Lambda code is read from `target/lambda/<bin>/bootstrap` (build with
`cargo lambda build --release --arm64`). Set `CDK_PLACEHOLDER=1` to synth without the binaries.
"""

import json
import os
from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
)
from aws_cdk import aws_apigatewayv2 as apigw
from aws_cdk import aws_apigatewayv2_authorizers as apigw_authz
from aws_cdk import aws_apigatewayv2_integrations as apigw_integ
from aws_cdk import aws_cognito as cognito
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_opensearchserverless as aoss
from aws_cdk import aws_sqs as sqs
from constructs import Construct

REPO_ROOT = Path(__file__).resolve().parents[2]
PLACEHOLDER = REPO_ROOT / "cdk" / "placeholder"


def lambda_code(name: str) -> lambda_.Code:
    """Resolve a Lambda's code asset, falling back to a placeholder only when explicitly allowed."""
    real = REPO_ROOT / "target" / "lambda" / name
    if (real / "bootstrap").exists():
        return lambda_.Code.from_asset(str(real))
    if os.environ.get("CDK_PLACEHOLDER"):
        return lambda_.Code.from_asset(str(PLACEHOLDER))
    raise RuntimeError(
        f"Lambda artifact for '{name}' not found at {real}. Run "
        "`cargo lambda build --release --arm64` first, or set CDK_PLACEHOLDER=1 to synth "
        "without the compiled binaries."
    )


class TadsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, prefix: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.prefix = prefix

        self._build_key()
        self._build_tables()
        self._build_queues()
        self._build_auth()
        self._build_functions()
        self._grant_permissions()
        self._wire_event_sources()
        self._build_search()
        self._build_http_api()
        self._outputs()

    # -- KMS ----------------------------------------------------------------------------------
    def _build_key(self) -> None:
        """One customer-managed CMK encrypts all data at rest (DynamoDB, SQS, Lambda env vars).

        A single shared key keeps IAM/grant wiring simple; CDK ``grant_*`` helpers add the
        matching ``kms:Decrypt``/``GenerateDataKey`` permissions to each consumer automatically.
        """
        self.key = kms.Key(
            self,
            "DataKey",
            alias=f"alias/{self.prefix}",
            description=f"{self.prefix} data-at-rest CMK (DynamoDB, SQS, Lambda env)",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.DESTROY,  # dev; use RETAIN in production
        )

    # -- DynamoDB -----------------------------------------------------------------------------
    def _build_tables(self) -> None:
        common = dict(
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=self.key,
            removal_policy=RemovalPolicy.DESTROY,  # dev; use RETAIN in production
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
        )

        self.schemas = dynamodb.Table(
            self,
            "SchemasTable",
            table_name=f"{self.prefix}-schemas",
            partition_key=dynamodb.Attribute(name="schema_id", type=dynamodb.AttributeType.STRING),
            **common,
        )
        self.schemas.add_global_secondary_index(
            index_name="external_id-index",
            partition_key=dynamodb.Attribute(name="external_id", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        self.tracks = dynamodb.Table(
            self,
            "TracksTable",
            table_name=f"{self.prefix}-tracks",
            partition_key=dynamodb.Attribute(name="track_id", type=dynamodb.AttributeType.STRING),
            **common,
        )
        self.tracks.add_global_secondary_index(
            index_name="schema_id-index",
            partition_key=dynamodb.Attribute(name="schema_id", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.ALL,
        )
        self.tracks.add_global_secondary_index(
            index_name="entity_id-index",
            partition_key=dynamodb.Attribute(name="entity_id", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        self.events = dynamodb.Table(
            self,
            "EventsTable",
            table_name=f"{self.prefix}-events",
            partition_key=dynamodb.Attribute(name="track_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            **common,
        )

        self.webhooks = dynamodb.Table(
            self,
            "WebhooksTable",
            table_name=f"{self.prefix}-webhooks",
            partition_key=dynamodb.Attribute(name="webhook_id", type=dynamodb.AttributeType.STRING),
            **common,
        )

        self.delete_requests = dynamodb.Table(
            self,
            "DeleteRequestsTable",
            table_name=f"{self.prefix}-delete-requests",
            partition_key=dynamodb.Attribute(name="request_id", type=dynamodb.AttributeType.STRING),
            time_to_live_attribute="expiry",
            **common,
        )

    # -- SQS ----------------------------------------------------------------------------------
    def _build_queues(self) -> None:
        self.delete_dlq = sqs.Queue(
            self,
            "DeleteDLQ",
            queue_name=f"{self.prefix}-delete-dlq",
            retention_period=Duration.days(14),
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=self.key,
        )
        self.delete_queue = sqs.Queue(
            self,
            "DeleteQueue",
            queue_name=f"{self.prefix}-delete",
            visibility_timeout=Duration.seconds(180),
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=self.key,
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=5, queue=self.delete_dlq),
        )

        # Shared async-invocation DLQ for every Lambda (failed async events land here).
        self.lambda_dlq = sqs.Queue(
            self,
            "LambdaDLQ",
            queue_name=f"{self.prefix}-lambda-dlq",
            retention_period=Duration.days(14),
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=self.key,
        )

    # -- Cognito ------------------------------------------------------------------------------
    def _build_auth(self) -> None:
        self.user_pool = cognito.UserPool(
            self,
            "UserPool",
            user_pool_name=f"{self.prefix}-users",
            self_sign_up_enabled=False,
            removal_policy=RemovalPolicy.DESTROY,  # dev
        )
        self.user_pool_client = self.user_pool.add_client(
            "ApiClient",
            user_pool_client_name=f"{self.prefix}-api-client",
            generate_secret=False,
            auth_flows=cognito.AuthFlow(user_password=True, user_srp=True),
        )

    # -- Lambda functions ---------------------------------------------------------------------
    def _common_env(self) -> dict:
        return {
            "TADS__TABLES__SCHEMAS": self.schemas.table_name,
            "TADS__TABLES__TRACKS": self.tracks.table_name,
            "TADS__TABLES__EVENTS": self.events.table_name,
            "TADS__TABLES__WEBHOOKS": self.webhooks.table_name,
            "TADS__TABLES__DELETE_REQUESTS": self.delete_requests.table_name,
            "TADS__DELETE_QUEUE_URL": self.delete_queue.queue_url,
            # service.json fields (no YAML file is bundled into the Lambda image)
            "TADS__SERVICE__TYPE": "urn:x-tads:service:reference.rust",
            "TADS__SERVICE__API_VERSION": "0.1",
            "TADS__SERVICE__NAME": "TADS Reference (Rust)",
            "TADS__SERVICE__SERVICE_VERSION": "0.1.0",
            "RUST_LOG": "info,tads=debug",
        }

    def _make_fn(
        self,
        construct_id: str,
        bin_name: str,
        *,
        memory: int = 256,
        timeout: int = 30,
        reserved_concurrency: int = 20,
        extra_env: dict | None = None,
    ) -> lambda_.Function:
        env = self._common_env()
        if extra_env:
            env.update(extra_env)
        # Optional ADOT layer for OTLP->X-Ray span export (ARN is region-specific).
        layers = []
        adot_arn = self.node.try_get_context("adot_layer_arn")
        if adot_arn:
            layers.append(
                lambda_.LayerVersion.from_layer_version_arn(self, f"{construct_id}Adot", adot_arn)
            )
            env["AWS_LAMBDA_EXEC_WRAPPER"] = "/opt/otel-handler"

        return lambda_.Function(
            self,
            construct_id,
            function_name=f"{self.prefix}-{bin_name}",
            runtime=lambda_.Runtime.PROVIDED_AL2023,
            architecture=lambda_.Architecture.ARM_64,
            handler="bootstrap",
            code=lambda_code(bin_name),
            memory_size=memory,
            timeout=Duration.seconds(timeout),
            environment=env,
            # Env vars are encrypted at rest with the stack CMK (CDK grants the role decrypt).
            environment_encryption=self.key,
            # Async-invocation failures go to a shared DLQ instead of being dropped.
            dead_letter_queue=self.lambda_dlq,
            # Cap per-function concurrency (cost / blast-radius control). Total across the 9
            # functions must stay below the account's unreserved pool (default 1000, keeps >=100).
            reserved_concurrent_executions=reserved_concurrency,
            tracing=lambda_.Tracing.ACTIVE,
            layers=layers or None,
        )

    def _build_functions(self) -> None:
        self.fn_service = self._make_fn("ServiceFn", "service")
        self.fn_schemas = self._make_fn("SchemasFn", "schemas")
        self.fn_tracks = self._make_fn("TracksFn", "tracks")
        self.fn_events = self._make_fn("EventsFn", "events")
        self.fn_webhooks = self._make_fn("WebhooksFn", "webhooks")
        self.fn_delete_requests = self._make_fn("DeleteRequestsFn", "delete-requests")

        self.fn_delete_worker = self._make_fn(
            "DeleteWorkerFn", "delete-worker", memory=512, timeout=120
        )
        self.fn_dispatcher = self._make_fn(
            "WebhookDispatcherFn", "webhook-dispatcher", memory=256, timeout=60
        )
        self.fn_indexer = self._make_fn(
            "SearchIndexerFn", "search-indexer", memory=256, timeout=60
        )

    # -- IAM grants (least privilege) ---------------------------------------------------------
    def _grant_permissions(self) -> None:
        # HTTP handlers
        self.schemas.grant_read_write_data(self.fn_schemas)
        self.tracks.grant_read_data(self.fn_schemas)  # references_schema guard

        self.tracks.grant_read_write_data(self.fn_tracks)
        self.delete_requests.grant_read_write_data(self.fn_tracks)
        self.delete_queue.grant_send_messages(self.fn_tracks)

        self.events.grant_read_write_data(self.fn_events)
        self.tracks.grant_read_data(self.fn_events)
        self.schemas.grant_read_data(self.fn_events)
        self.delete_requests.grant_read_write_data(self.fn_events)
        self.delete_queue.grant_send_messages(self.fn_events)

        self.delete_requests.grant_read_data(self.fn_delete_requests)
        self.webhooks.grant_read_write_data(self.fn_webhooks)
        # fn_service needs no table access.

        # Workers
        self.events.grant_read_write_data(self.fn_delete_worker)
        self.tracks.grant_read_write_data(self.fn_delete_worker)
        self.delete_requests.grant_read_write_data(self.fn_delete_worker)

        self.webhooks.grant_read_write_data(self.fn_dispatcher)  # list_all + status update
        # fn_indexer OpenSearch access is granted in _build_search.

    # -- Event source mappings ----------------------------------------------------------------
    def _wire_event_sources(self) -> None:
        self.fn_delete_worker.add_event_source(
            sources.SqsEventSource(self.delete_queue, batch_size=10)
        )

        # Both the dispatcher and the indexer consume each table's stream (2 consumers/table).
        for table in (self.schemas, self.tracks, self.events):
            for fn in (self.fn_dispatcher, self.fn_indexer):
                fn.add_event_source(
                    sources.DynamoEventSource(
                        table,
                        starting_position=lambda_.StartingPosition.LATEST,
                        batch_size=25,
                        retry_attempts=3,
                        bisect_batch_on_error=True,
                        report_batch_item_failures=True,
                    )
                )

    # -- OpenSearch Serverless ----------------------------------------------------------------
    def _build_search(self) -> None:
        name = f"{self.prefix}-search"[:32]
        collection_resource = [f"collection/{name}"]

        encryption = aoss.CfnSecurityPolicy(
            self,
            "SearchEncryptionPolicy",
            name=f"{name}-enc"[:32],
            type="encryption",
            policy=json.dumps(
                {"Rules": [{"ResourceType": "collection", "Resource": collection_resource}], "AWSOwnedKey": True}
            ),
        )
        network = aoss.CfnSecurityPolicy(
            self,
            "SearchNetworkPolicy",
            name=f"{name}-net"[:32],
            type="network",
            policy=json.dumps(
                [
                    {
                        "Rules": [
                            {"ResourceType": "collection", "Resource": collection_resource},
                            {"ResourceType": "dashboard", "Resource": collection_resource},
                        ],
                        "AllowFromPublic": True,
                    }
                ]
            ),
        )

        self.collection = aoss.CfnCollection(
            self, "SearchCollection", name=name, type="SEARCH"
        )
        self.collection.node.add_dependency(encryption)
        self.collection.node.add_dependency(network)

        # Data-plane access for the indexer's execution role.
        indexer_role_arn = self.fn_indexer.role.role_arn
        data_access = aoss.CfnAccessPolicy(
            self,
            "SearchDataAccessPolicy",
            name=f"{name}-data"[:32],
            type="data",
            policy=json.dumps(
                [
                    {
                        "Rules": [
                            {
                                "ResourceType": "index",
                                "Resource": [f"index/{name}/*"],
                                "Permission": [
                                    "aoss:CreateIndex",
                                    "aoss:UpdateIndex",
                                    "aoss:DescribeIndex",
                                    "aoss:ReadDocument",
                                    "aoss:WriteDocument",
                                ],
                            },
                            {
                                "ResourceType": "collection",
                                "Resource": collection_resource,
                                "Permission": ["aoss:CreateCollectionItems", "aoss:DescribeCollectionItems"],
                            },
                        ],
                        "Principal": [indexer_role_arn],
                    }
                ]
            ),
        )
        data_access.node.add_dependency(self.collection)

        # IAM allow to reach the data-plane endpoint.
        self.fn_indexer.add_to_role_policy(
            iam.PolicyStatement(
                actions=["aoss:APIAccessAll"],
                resources=[self.collection.attr_arn],
            )
        )
        self.fn_indexer.add_environment(
            "TADS_OPENSEARCH_ENDPOINT", self.collection.attr_collection_endpoint
        )

    # -- HTTP API -----------------------------------------------------------------------------
    def _build_http_api(self) -> None:
        self.http_api = apigw.HttpApi(
            self,
            "HttpApi",
            api_name=f"{self.prefix}-api",
            create_default_stage=True,
        )

        # Access logging on the default stage -> CloudWatch Logs (KMS-encrypted with the CMK).
        # CloudWatch Logs needs explicit key-policy access to encrypt the log group.
        self.key.add_to_resource_policy(
            iam.PolicyStatement(
                principals=[iam.ServicePrincipal(f"logs.{self.region}.amazonaws.com")],
                actions=[
                    "kms:Encrypt*",
                    "kms:Decrypt*",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:Describe*",
                ],
                resources=["*"],
                conditions={
                    "ArnLike": {
                        "kms:EncryptionContext:aws:logs:arn": (
                            f"arn:aws:logs:{self.region}:{self.account}:log-group:*"
                        )
                    }
                },
            )
        )
        access_logs = logs.LogGroup(
            self,
            "ApiAccessLogs",
            log_group_name=f"/aws/apigateway/{self.prefix}-api/access",
            retention=logs.RetentionDays.ONE_MONTH,
            encryption_key=self.key,
            removal_policy=RemovalPolicy.DESTROY,  # dev
        )
        default_stage = self.http_api.default_stage.node.default_child
        default_stage.access_log_settings = apigw.CfnStage.AccessLogSettingsProperty(
            destination_arn=access_logs.log_group_arn,
            format=json.dumps(
                {
                    "requestId": "$context.requestId",
                    "ip": "$context.identity.sourceIp",
                    "requestTime": "$context.requestTime",
                    "httpMethod": "$context.httpMethod",
                    "routeKey": "$context.routeKey",
                    "path": "$context.path",
                    "status": "$context.status",
                    "protocol": "$context.protocol",
                    "responseLength": "$context.responseLength",
                    "integrationError": "$context.integrationErrorMessage",
                }
            ),
        )

        authorizer = apigw_authz.HttpUserPoolAuthorizer(
            "JwtAuthorizer",
            self.user_pool,
            user_pool_clients=[self.user_pool_client],
        )

        def route(path: str, fn: lambda_.Function, integ_id: str) -> None:
            self.http_api.add_routes(
                path=path,
                methods=[apigw.HttpMethod.ANY],
                integration=apigw_integ.HttpLambdaIntegration(integ_id, fn),
                authorizer=authorizer,
            )

        # Most-specific routes win in HTTP API regardless of declaration order.
        route("/", self.fn_service, "RootInteg")
        route("/service", self.fn_service, "ServiceInteg")
        route("/service/webhooks", self.fn_webhooks, "WebhooksInteg")
        route("/service/webhooks/{proxy+}", self.fn_webhooks, "WebhooksProxyInteg")

        route("/schemas", self.fn_schemas, "SchemasInteg")
        route("/schemas/{proxy+}", self.fn_schemas, "SchemasProxyInteg")

        route("/tracks/{track_id}/events", self.fn_events, "EventsInteg")
        route("/tracks", self.fn_tracks, "TracksInteg")
        route("/tracks/{proxy+}", self.fn_tracks, "TracksProxyInteg")

        route("/track-delete-requests", self.fn_delete_requests, "DeleteReqInteg")
        route("/track-delete-requests/{proxy+}", self.fn_delete_requests, "DeleteReqProxyInteg")

    def _outputs(self) -> None:
        cdk.CfnOutput(self, "ApiEndpoint", value=self.http_api.api_endpoint)
        cdk.CfnOutput(self, "UserPoolId", value=self.user_pool.user_pool_id)
        cdk.CfnOutput(self, "UserPoolClientId", value=self.user_pool_client.user_pool_client_id)
        cdk.CfnOutput(self, "SearchEndpoint", value=self.collection.attr_collection_endpoint)
