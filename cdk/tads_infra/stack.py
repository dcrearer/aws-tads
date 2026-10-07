"""TADS API infrastructure stack (Aurora PostgreSQL backend).

Provisions, per the project design:
- A VPC (isolated subnets, NO NAT) with interface endpoints for Secrets Manager, SQS, KMS, STS
- Aurora PostgreSQL Serverless v2 + RDS Proxy (TLS), credentials in Secrets Manager, CMK-encrypted
- A one-off `migrate` Lambda (sqlx migrations) run via a CloudFormation custom resource
- An SQS delete queue (+ DLQ) and a shared Lambda async-invocation DLQ
- Rust Lambdas (`provided.al2023`, arm64): 6 HTTP handlers + delete-worker + outbox-driven
  webhook-dispatcher (EventBridge-scheduled) + a custom Lambda authorizer (out of the VPC)
- An HTTP API Gateway fronted by the Lambda authorizer, with access logging
- A customer-managed KMS CMK encrypting all data at rest

The webhook/search change feed is a transactional **outbox** drained by the dispatcher (no
DynamoDB Streams). Search is served natively by Postgres (no OpenSearch).

Lambda code is read from `target/lambda/<bin>/bootstrap` (build with `scripts/build-lambdas.sh`).
Set `CDK_PLACEHOLDER=1` to synth without the binaries.
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
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_rds as rds
from aws_cdk import aws_sqs as sqs
from aws_cdk import custom_resources as cr
from constructs import Construct

REPO_ROOT = Path(__file__).resolve().parents[2]
PLACEHOLDER = REPO_ROOT / "cdk" / "placeholder"

DB_NAME = "tads"
DB_USER = "tads"


def lambda_code(name: str) -> lambda_.Code:
    """Resolve a Lambda's code asset, falling back to a placeholder only when explicitly allowed."""
    real = REPO_ROOT / "target" / "lambda" / name
    if (real / "bootstrap").exists():
        return lambda_.Code.from_asset(str(real))
    if os.environ.get("CDK_PLACEHOLDER"):
        return lambda_.Code.from_asset(str(PLACEHOLDER))
    raise RuntimeError(
        f"Lambda artifact for '{name}' not found at {real}. Run "
        "`./scripts/build-lambdas.sh` first, or set CDK_PLACEHOLDER=1 to synth "
        "without the compiled binaries."
    )


class TadsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, prefix: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.prefix = prefix

        self._build_key()
        self._build_network()
        self._build_database()
        self._build_queues()
        self._build_auth()
        self._build_functions()
        self._grant_permissions()
        self._wire_event_sources()
        self._build_migrate()
        self._build_http_api()
        self._outputs()

    # -- KMS ----------------------------------------------------------------------------------
    def _build_key(self) -> None:
        """One customer-managed CMK encrypts all data at rest (Aurora, SQS, Lambda env, logs)."""
        self.key = kms.Key(
            self,
            "DataKey",
            alias=f"alias/{self.prefix}",
            description=f"{self.prefix} data-at-rest CMK",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.DESTROY,  # dev; use RETAIN in production
        )

    # -- Networking ---------------------------------------------------------------------------
    def _build_network(self) -> None:
        """Isolated VPC (no NAT). DB-touching Lambdas, Aurora and RDS Proxy live in private
        subnets; interface endpoints give them Secrets Manager / SQS / KMS / STS without egress.
        The authorizer Lambda stays OUT of the VPC (it needs public egress to Cognito JWKS)."""
        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                )
            ],
        )
        for name, svc in [
            ("SecretsManagerEndpoint", ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER),
            ("SqsEndpoint", ec2.InterfaceVpcEndpointAwsService.SQS),
            ("KmsEndpoint", ec2.InterfaceVpcEndpointAwsService.KMS),
            ("StsEndpoint", ec2.InterfaceVpcEndpointAwsService.STS),
        ]:
            self.vpc.add_interface_endpoint(name, service=svc)

        # One shared security group for all in-VPC Lambdas.
        self.lambda_sg = ec2.SecurityGroup(
            self, "LambdaSg", vpc=self.vpc, description="TADS in-VPC Lambdas", allow_all_outbound=True
        )

    # -- Aurora + RDS Proxy -------------------------------------------------------------------
    def _build_database(self) -> None:
        self.cluster = rds.DatabaseCluster(
            self,
            "Db",
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_16_4
            ),
            credentials=rds.Credentials.from_generated_secret(DB_USER, encryption_key=self.key),
            writer=rds.ClusterInstance.serverless_v2("writer"),
            serverless_v2_min_capacity=0.5,  # floor >0 avoids auto-pause cold starts
            serverless_v2_max_capacity=4,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            default_database_name=DB_NAME,
            storage_encryption_key=self.key,
            iam_authentication=True,  # available for future IAM-token auth; app uses the secret today
            monitoring_interval=Duration.seconds(60),  # enhanced monitoring
            removal_policy=RemovalPolicy.DESTROY,  # dev
        )
        self.db_secret = self.cluster.secret

        self.proxy = self.cluster.add_proxy(
            "Proxy",
            secrets=[self.db_secret],
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            require_tls=True,
            db_proxy_name=f"{self.prefix}-proxy",
        )
        # Lambdas -> proxy -> cluster on 5432.
        self.proxy.connections.allow_from(self.lambda_sg, ec2.Port.tcp(5432))
        self.cluster.connections.allow_from(self.proxy, ec2.Port.tcp(5432))

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
        self.lambda_dlq = sqs.Queue(
            self,
            "LambdaDLQ",
            queue_name=f"{self.prefix}-lambda-dlq",
            retention_period=Duration.days(14),
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=self.key,
        )

    # -- Cognito (OAuth2 client_credentials, M2M) ---------------------------------------------
    def _build_auth(self) -> None:
        self.user_pool = cognito.UserPool(
            self,
            "UserPool",
            user_pool_name=f"{self.prefix}-users",
            self_sign_up_enabled=False,
            removal_policy=RemovalPolicy.DESTROY,  # dev
        )
        domain_prefix = self.node.try_get_context("cognito_domain_prefix") or f"{self.prefix}-{self.account}"
        self.user_pool_domain = self.user_pool.add_domain(
            "Domain",
            cognito_domain=cognito.CognitoDomainOptions(domain_prefix=domain_prefix),
        )
        self.token_url = f"https://{domain_prefix}.auth.{self.region}.amazoncognito.com/oauth2/token"

        scope_read = cognito.ResourceServerScope(scope_name="read", scope_description="Read access")
        scope_write = cognito.ResourceServerScope(scope_name="write", scope_description="Write access")
        scope_delete = cognito.ResourceServerScope(scope_name="delete", scope_description="Delete access")
        resource_server = self.user_pool.add_resource_server(
            "ResourceServer",
            identifier="tads-api",
            scopes=[scope_read, scope_write, scope_delete],
        )
        self.user_pool_client = self.user_pool.add_client(
            "ApiClient",
            user_pool_client_name=f"{self.prefix}-api-client",
            generate_secret=True,
            o_auth=cognito.OAuthSettings(
                flows=cognito.OAuthFlows(client_credentials=True),
                scopes=[
                    cognito.OAuthScope.resource_server(resource_server, scope_read),
                    cognito.OAuthScope.resource_server(resource_server, scope_write),
                    cognito.OAuthScope.resource_server(resource_server, scope_delete),
                ],
            ),
        )
        self.user_pool_client.node.add_dependency(resource_server)

    # -- Lambda functions ---------------------------------------------------------------------
    def _common_env(self) -> dict:
        return {
            "TADS__DATABASE__PROXY_ENDPOINT": self.proxy.endpoint,
            "TADS__DATABASE__DB_NAME": DB_NAME,
            "TADS__DATABASE__USERNAME": DB_USER,
            "TADS__DATABASE__SECRET_ARN": self.db_secret.secret_arn,
            "TADS__DELETE_QUEUE_URL": self.delete_queue.queue_url,
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
        in_vpc: bool = True,
        extra_env: dict | None = None,
    ) -> lambda_.Function:
        env = self._common_env()
        if extra_env:
            env.update(extra_env)
        layers = []
        adot_arn = self.node.try_get_context("adot_layer_arn")
        if adot_arn:
            layers.append(
                lambda_.LayerVersion.from_layer_version_arn(self, f"{construct_id}Adot", adot_arn)
            )
            env["AWS_LAMBDA_EXEC_WRAPPER"] = "/opt/otel-handler"

        fn = lambda_.Function(
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
            environment_encryption=self.key,
            dead_letter_queue=self.lambda_dlq,
            reserved_concurrent_executions=reserved_concurrency,
            tracing=lambda_.Tracing.ACTIVE,
            layers=layers or None,
            vpc=self.vpc if in_vpc else None,
            vpc_subnets=(
                ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED) if in_vpc else None
            ),
            security_groups=[self.lambda_sg] if in_vpc else None,
        )
        if in_vpc:
            # In-VPC functions connect to Aurora through the proxy using the generated secret.
            self.db_secret.grant_read(fn)
        return fn

    def _build_functions(self) -> None:
        self.fn_service = self._make_fn("ServiceFn", "service")
        self.fn_schemas = self._make_fn("SchemasFn", "schemas")
        self.fn_tracks = self._make_fn("TracksFn", "tracks")
        self.fn_events = self._make_fn("EventsFn", "events")
        self.fn_webhooks = self._make_fn("WebhooksFn", "webhooks")
        self.fn_delete_requests = self._make_fn("DeleteRequestsFn", "delete-requests")

        self.fn_delete_worker = self._make_fn("DeleteWorkerFn", "delete-worker", memory=512, timeout=120)
        # Outbox-driven webhook dispatcher (EventBridge-scheduled; reads outbox + webhooks).
        self.fn_dispatcher = self._make_fn("WebhookDispatcherFn", "webhook-dispatcher", memory=256, timeout=60)

        # Custom Lambda authorizer — OUT of the VPC (needs public egress to Cognito JWKS).
        self.fn_authorizer = self._make_fn(
            "AuthorizerFn",
            "authorizer",
            memory=256,
            timeout=10,
            in_vpc=False,
            extra_env={
                "TADS_JWT_ISSUER": f"https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool.user_pool_id}",
                "TADS_CLIENT_ID": self.user_pool_client.user_pool_client_id,
            },
        )

    # -- IAM grants (least privilege) ---------------------------------------------------------
    def _grant_permissions(self) -> None:
        # DB access is granted in _make_fn (secret read + SG path to the proxy). The only extra
        # grants are SQS send for the handlers that enqueue delete-request work.
        self.delete_queue.grant_send_messages(self.fn_tracks)
        self.delete_queue.grant_send_messages(self.fn_events)

    # -- Event sources / schedules ------------------------------------------------------------
    def _wire_event_sources(self) -> None:
        self.fn_delete_worker.add_event_source(sources.SqsEventSource(self.delete_queue, batch_size=10))
        # Drain the transactional outbox on a schedule (replaces DynamoDB Streams).
        events.Rule(
            self,
            "DispatchSchedule",
            schedule=events.Schedule.rate(Duration.minutes(1)),
            targets=[targets.LambdaFunction(self.fn_dispatcher)],
        )

    # -- Migrations (custom resource) ---------------------------------------------------------
    def _build_migrate(self) -> None:
        self.fn_migrate = self._make_fn("MigrateFn", "migrate", timeout=120, reserved_concurrency=1)
        provider = cr.Provider(self, "MigrateProvider", on_event_handler=self.fn_migrate)
        resource = cdk.CustomResource(
            self,
            "RunMigrations",
            service_token=provider.service_token,
            # Re-run when the migration bundle changes (bump this to force a re-run).
            properties={"migration_version": "0001"},
        )
        resource.node.add_dependency(self.proxy)

    # -- HTTP API -----------------------------------------------------------------------------
    def _build_http_api(self) -> None:
        self.http_api = apigw.HttpApi(
            self,
            "HttpApi",
            api_name=f"{self.prefix}-api",
            create_default_stage=True,
        )

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

        authorizer = apigw_authz.HttpLambdaAuthorizer(
            "LambdaAuthorizer",
            self.fn_authorizer,
            authorizer_name=f"{self.prefix}-authorizer",
            response_types=[apigw_authz.HttpLambdaResponseType.SIMPLE],
            identity_source=["$request.header.Authorization"],
            results_cache_ttl=Duration.minutes(5),
        )

        def route(path: str, fn: lambda_.Function, integ_id: str) -> None:
            self.http_api.add_routes(
                path=path,
                methods=[apigw.HttpMethod.ANY],
                integration=apigw_integ.HttpLambdaIntegration(integ_id, fn),
                authorizer=authorizer,
            )

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
        client_id = self.user_pool_client.user_pool_client_id
        cdk.CfnOutput(self, "ApiEndpoint", value=self.http_api.api_endpoint)
        cdk.CfnOutput(self, "UserPoolId", value=self.user_pool.user_pool_id)
        cdk.CfnOutput(self, "UserPoolClientId", value=client_id)
        cdk.CfnOutput(self, "DbProxyEndpoint", value=self.proxy.endpoint)
        cdk.CfnOutput(
            self,
            "JwtIssuer",
            value=f"https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool.user_pool_id}",
        )
        cdk.CfnOutput(self, "TokenUrl", value=self.token_url)
        cdk.CfnOutput(self, "OAuthScopes", value="tads-api/read tads-api/write tads-api/delete")
        cdk.CfnOutput(
            self,
            "ClientSecret",
            value=self.user_pool_client.user_pool_client_secret.unsafe_unwrap(),
            description="OAuth2 client secret (paste into Insomnia as client_secret)",
        )
        cdk.CfnOutput(
            self,
            "GetTokenCommand",
            value=(
                f"curl -s -X POST {self.token_url} "
                f"-H 'Content-Type: application/x-www-form-urlencoded' "
                f"-u {client_id}:{self.user_pool_client.user_pool_client_secret.unsafe_unwrap()} "
                f"--data-urlencode grant_type=client_credentials "
                f"--data-urlencode 'scope=tads-api/read tads-api/write tads-api/delete' "
                f"| python3 -c 'import json,sys;print(json.load(sys.stdin)[\"access_token\"])'"
            ),
        )
